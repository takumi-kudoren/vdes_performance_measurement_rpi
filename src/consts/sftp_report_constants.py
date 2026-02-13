"""受信側レポート配置に関する定数を定義する。"""

# レポートファイルを配置するサーバーディレクトリ。
REMOTE_REPORT_DIRECTORY = "/home/admin/vdes_web_sys/src/resources/nmea/performance/json"

# 既存呼び出しとの互換性維持用。
REMOTE_REPORT_JSON_DIRECTORY = REMOTE_REPORT_DIRECTORY

# アップロード失敗時の再試行間隔（秒）。
POLL_INTERVAL_SECONDS = 5
