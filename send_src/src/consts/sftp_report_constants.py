"""受信側パフォーマンス参照に関する定数を定義する。"""

# 受信側パフォーマンスjsonlが配置されるサーバーディレクトリ。
REMOTE_REPORT_JSON_DIRECTORY = "/home/admin/vdes_web_sys/src/resources/nmea/report/json"

# 受信側パフォーマンスファイル名の規則。
REPORT_FILE_PREFIX = "REPORT_"
REPORT_FILE_SUFFIX = ".jsonl"

# 取得待機時の再試行間隔（秒）。
POLL_INTERVAL_SECONDS = 5
