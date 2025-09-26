<?php
declare(strict_types=1);
error_reporting(E_ALL);
ini_set('display_errors', '0');

require __DIR__ . '/vendor/autoload.php';

use Jmrashed\Zkteco\Lib\ZKTeco;

date_default_timezone_set('Asia/Tehran');

/* ================== تنظیمات ================== */
const ZK_IP        = '46.100.10.217';
const ZK_PORT      = 4370;

const STORAGE_DIR  = __DIR__ . '/storage';
const SQLITE_PATH  = STORAGE_DIR . '/attendance.sqlite';

const API_URL      = 'https://portal.sobhe-roshan.ir/api/v1/ElectronicAttendance';
const API_PASS     = '1234';      // اگر لازم نیست خالی بگذارید ''
const BATCH_LIMIT  = 500;         // حداکثر ارسال در هر اجرا
const CURL_TIMEOUT = 15;          // ثانیه
/* ============================================ */

/* ====== ابزار DB ====== */
function db(): PDO {
    if (!is_dir(STORAGE_DIR)) {
        if (!mkdir(STORAGE_DIR, 0777, true) && !is_dir(STORAGE_DIR)) {
            throw new RuntimeException('Cannot create storage dir');
        }
    }
    $pdo = new PDO('sqlite:' . SQLITE_PATH);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    ensureSchema($pdo);
    return $pdo;
}

function columnExists(PDO $pdo, string $table, string $column): bool {
    $stmt = $pdo->query("PRAGMA table_info($table)");
    $cols = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
    foreach ($cols as $c) {
        if (isset($c['name']) && strcasecmp($c['name'], $column) === 0) {
            return true;
        }
    }
    return false;
}

function ensureSchema(PDO $pdo): void {
    // جدول پایه
    $pdo->exec("
        CREATE TABLE IF NOT EXISTS attendance_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uid        TEXT NOT NULL,
            emp_id     TEXT,
            status     INTEGER,
            ts         TEXT NOT NULL,
            server_id  TEXT NULL,
            raw_json   TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            sent_at    TEXT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(uid, ts)
        );
    ");
    $pdo->exec("CREATE UNIQUE INDEX IF NOT EXISTS ux_attendance_uid_ts ON attendance_logs (uid, ts);");

    // برای سازگاری اگر جدول قدیمی بود، ستون‌ها را اضافه کنیم
    foreach ([
        'emp_id' => "ALTER TABLE attendance_logs ADD COLUMN emp_id TEXT;",
        'status' => "ALTER TABLE attendance_logs ADD COLUMN status INTEGER;",
        'server_id' => "ALTER TABLE attendance_logs ADD COLUMN server_id TEXT NULL;",
        'raw_json' => "ALTER TABLE attendance_logs ADD COLUMN raw_json TEXT;",
        'created_at' => "ALTER TABLE attendance_logs ADD COLUMN created_at TEXT NOT NULL DEFAULT (datetime('now'));",
        'sent_at' => "ALTER TABLE attendance_logs ADD COLUMN sent_at TEXT NULL;",
        'updated_at' => "ALTER TABLE attendance_logs ADD COLUMN updated_at TEXT NOT NULL DEFAULT (datetime('now'));",
    ] as $col => $sql) {
        if (!columnExists($pdo, 'attendance_logs', $col)) {
            $pdo->exec($sql);
        }
    }
}

/* ====== دریافت از دستگاه ====== */
function fetchFromDevice(): array {
    $zk = new ZKTeco(ZK_IP, ZK_PORT);

    if (!$zk->connect()) {
        throw new RuntimeException("Cannot connect to device " . ZK_IP . ":" . ZK_PORT);
    }

    // اگر متدهای disable/enable وجود داشته باشند
    try { if (method_exists($zk, 'disableDevice')) { $zk->disableDevice(); } } catch (Throwable $e) {}

    $logs = [];
    try {
        // معمولاً: [ [uid, emp_id, status, datetime], ... ]
        $logs = $zk->getAttendance() ?? [];
    } catch (Throwable $e) {
        // بی‌صدا رد می‌کنیم؛ لااقل قطع ارتباط انجام شود
    }

    try { if (method_exists($zk, 'enableDevice')) { $zk->enableDevice(); } } catch (Throwable $e) {}
    $zk->disconnect();

    return $logs;
}

function mapAttendanceItem($att): ?array {
    // پشتیبانی از آرایه ایندکسی/کلیدی و آبجکت
    $uid = $emp_id = null;
    $status = null;
    $dt = null;

    if (is_array($att)) {
        // ایندکسی
        if (array_key_exists(0, $att)) { $uid    = (string)$att[0]; }
        if (array_key_exists(1, $att)) { $emp_id = (string)$att[1]; }
        if (array_key_exists(2, $att)) { $status = is_numeric($att[2]) ? (int)$att[2] : null; }
        if (array_key_exists(3, $att)) { $dt     = (string)$att[3]; }
        // کلیدی
        if ($uid    === null && isset($att['uid']))        $uid = (string)$att['uid'];
        if ($emp_id === null && isset($att['id']))         $emp_id = (string)$att['id'];
        if ($status === null && isset($att['status']))     $status = is_numeric($att['status']) ? (int)$att['status'] : null;
        if ($dt     === null && isset($att['timestamp']))  $dt = (string)$att['timestamp'];
        if ($dt     === null && isset($att['time']))       $dt = (string)$att['time'];
    } else {
        // آبجکت
        if (isset($att->uid))        $uid = (string)$att->uid;
        if (isset($att->id))         $emp_id = (string)$att->id;
        if (isset($att->status))     $status = is_numeric($att->status) ? (int)$att->status : null;
        if (isset($att->timestamp))  $dt = (string)$att->timestamp;
        if ($dt === null && isset($att->time)) $dt = (string)$att->time;
    }

    if (!$uid || !$dt) return null;

    $t = strtotime($dt);
    $ts = $t !== false ? date('Y-m-d H:i:s', $t) : (string)$dt;

    return [
        'uid'    => $uid,
        'emp_id' => $emp_id ?? '',
        'status' => $status,
        'ts'     => $ts,
        'raw'    => $att,
    ];
}

/* ====== درج در DB با جلوگیری از تکرار ====== */
function insertLogs(PDO $pdo, array $rawLogs): int {
    $stmt = $pdo->prepare("
        INSERT OR IGNORE INTO attendance_logs
            (uid, emp_id, status, ts, server_id, raw_json, created_at)
        VALUES
            (:uid, :emp_id, :status, :ts, NULL, :raw_json, :created_at)
    ");

    $now = date('Y-m-d H:i:s');
    $inserted = 0;

    $pdo->beginTransaction();
    try {
        foreach ($rawLogs as $att) {
            $m = mapAttendanceItem($att);
            if ($m === null) continue;

            $ok = $stmt->execute([
                ':uid'        => $m['uid'],
                ':emp_id'     => $m['emp_id'],
                ':status'     => $m['status'],
                ':ts'         => $m['ts'],
                ':raw_json'   => json_encode($m['raw'], JSON_UNESCAPED_UNICODE),
                ':created_at' => $now,
            ]);
            if ($ok && $stmt->rowCount() === 1) $inserted++;
        }
        $pdo->commit();
    } catch (Throwable $e) {
        $pdo->rollBack();
        throw $e;
    }
    return $inserted;
}

/* ====== ارسال به سرور ====== */
function fetchUnsent(PDO $pdo, int $limit = BATCH_LIMIT): array {
    $stmt = $pdo->prepare("
        SELECT id, uid, ts
        FROM attendance_logs
        WHERE server_id IS NULL
        ORDER BY ts ASC
        LIMIT :lim
    ");
    $stmt->bindValue(':lim', $limit, PDO::PARAM_INT);
    $stmt->execute();
    return $stmt->fetchAll(PDO::FETCH_ASSOC);
}

function markSent(PDO $pdo, int $id, string $serverId): void {
    $stmt = $pdo->prepare("
        UPDATE attendance_logs
        SET server_id = :sid, sent_at = datetime('now'), updated_at = datetime('now')
        WHERE id = :id
    ");
    $stmt->execute([':sid' => $serverId, ':id' => $id]);
}

function postAttendance(array $payload, int $maxRetries = 3, int $timeout = CURL_TIMEOUT): array {
    $headers = [
        'Accept: application/json',
        'Content-Type: application/json',
        'User-Agent: ZKSync-PHP/1.0'
    ];

    for ($attempt = 1; $attempt <= $maxRetries; $attempt++) {
        $ch = curl_init(API_URL);
        curl_setopt_array($ch, [
            CURLOPT_POST           => true,
            CURLOPT_HTTPHEADER     => $headers,
            CURLOPT_POSTFIELDS     => json_encode($payload, JSON_UNESCAPED_UNICODE),
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_CONNECTTIMEOUT => $timeout,
            CURLOPT_TIMEOUT        => $timeout,
            // اگر SSL self-signed دارید و خطا می‌گیرید (فقط محیط تست):
            // CURLOPT_SSL_VERIFYPEER => false,
            // CURLOPT_SSL_VERIFYHOST => 0,
        ]);
        $body = curl_exec($ch);
        $err  = curl_error($ch);
        $code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);

        if ($err) {
            if ($attempt < $maxRetries) { sleep($attempt); continue; }
            return ['ok'=>false, 'server_id'=>null, 'error'=>"Network error: $err"];
        }

        if ($code >= 200 && $code < 300) {
            $data = json_decode($body ?: "{}", true);
            $sid  = $data['res_id'] ?? $data['id'] ?? ($body ?: null);
            if (!$sid) return ['ok'=>false, 'server_id'=>null, 'error'=>'No server id in response'];
            return ['ok'=>true, 'server_id'=>(string)$sid, 'error'=>null];
        }

        if ($code == 409) { // Duplicate
            $data = json_decode($body ?: "{}", true);
            $sid  = $data['res_id'] ?? $data['id'] ?? 'DUPLICATE';
            return ['ok'=>true, 'server_id'=>(string)$sid, 'error'=>null];
        }

        if ($code == 422) {
            return ['ok'=>false, 'server_id'=>null, 'error'=>"422: ".($body ?: 'Unprocessable')];
        }

        if ($attempt < $maxRetries) { sleep($attempt); continue; }
        return ['ok'=>false, 'server_id'=>null, 'error'=>"$code: $body"];
    }

    return ['ok'=>false, 'server_id'=>null, 'error'=>'Unknown'];
}

function sendUnsentRows(PDO $pdo, int $rateLimitMs = 0): int {
    $rows = fetchUnsent($pdo);
    $sent = 0;

    foreach ($rows as $r) {
        $payload = [
            'user_id' => $r['uid'],
            'time'    => $r['ts'],
        ];
        if (API_PASS !== '') $payload['pass'] = API_PASS;

        $res = postAttendance($payload);
        if ($res['ok'] && $res['server_id']) {
            markSent($pdo, (int)$r['id'], (string)$res['server_id']);
            $sent++;
        } else {
            // برای دیباگ:
            // error_log("Send failed id={$r['id']}: ".$res['error']);
        }

        if ($rateLimitMs > 0) usleep($rateLimitMs * 1000);
    }

    return $sent;
}

/* =============== اجرا =============== */
try {
    $pdo = db();

    // 1) دریافت از دستگاه و ذخیره در DB (بدون تکرار)
    $rawLogs = fetchFromDevice();
    $inserted = insertLogs($pdo, $rawLogs);

    // 2) ارسال موارد ارسال‌نشده به سرور و علامت‌گذاری
    $sent = sendUnsentRows($pdo, 0);

    // 3) خروجی
    header('Content-Type: text/plain; charset=utf-8');
    $total = (int)$pdo->query("SELECT COUNT(*) FROM attendance_logs")->fetchColumn();
    echo "Inserted now: $inserted\nSent to server: $sent\nTotal rows: $total\n";

} catch (Throwable $e) {
    http_response_code(500);
    header('Content-Type: text/plain; charset=utf-8');
    echo "ERROR: " . $e->getMessage() . "\n";
}
