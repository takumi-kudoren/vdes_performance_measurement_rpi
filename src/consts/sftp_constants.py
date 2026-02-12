"""SFTP接続に関する設定をまとめる。"""

# 接続先の基本情報。
HOST = "54.249.126.94"
PORT = 22
USERNAME = "cl-nmea-sftpuser"

# 秘密鍵ファイルのパス。ホームディレクトリ基準の相対パスを指定する。
# linux環境では `/etc/ssh/cl-nmea-sftpuser.key` を指す。
PRIVATE_KEY_PATH = "/etc/ssh/cl-nmea-sftpuser.key"

# 秘密鍵のパスフレーズ。不要ならNoneにする。
PRIVATE_KEY_PASSPHRASE = None

# 接続タイムアウト（秒）。
CONNECT_TIMEOUT_SECONDS = 30
