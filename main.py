import signal
import enum
import threading
import socket
import time
import pathlib
import json
from os import (
    environ,
)
from io import (
    DEFAULT_BUFFER_SIZE,
)
import dotenv
import requests
from watchdog.events import (
    FileSystemEventHandler,
    FileModifiedEvent,
)
from watchdog.observers.api import (
    ObservedWatch,
)
from watchdog.observers import (
    Observer,
)



DATASTORE = {}
DATASTORE_PATH = "datastore.json"
def load_datastore ():
    global DATASTORE
    ds = pathlib.Path(DATASTORE_PATH)
    if not ds.exists():
        with ds.open("w") as fp:
            json.dump({}, fp)
    with ds.open("r") as fp:
        DATASTORE = json.load(fp)

def save_datastore ():
    global DATASTORE, FILEWATCHER
    FILEWATCHER.unschedule_all()
    try:
        ds = pathlib.Path(DATASTORE_PATH)
        with ds.open("r+") as fp:
            json.dump(DATASTORE, fp)
    finally:
        FILEWATCHER.schedule(ModifiedHandler(), "./", recursive=False)


CONFIG_PATH = ".env"
def load_config ():
    global DATASTORE_PATH
    dotenv.load_dotenv(override=True)
    DATASTORE_PATH = environ["CSORSE_MASTERSERVER_DATASTORE_PATH"]


class ModifiedHandler (FileSystemEventHandler):
    def on_modified(self, event: FileModifiedEvent):
        if type(event) is not FileModifiedEvent:
            return
        src_path = pathlib.Path(event.src_path).resolve(False)
        if src_path == pathlib.Path(CONFIG_PATH).resolve(False):
            print(f"{CONFIG_PATH} has been modified. Reload now...")
            load_config()
        elif src_path == pathlib.Path(DATASTORE_PATH).resolve(False):
            print(f"{DATASTORE_PATH} has been modified. Reload now...")
            load_datastore()

FILEWATCHER: Observer = None
def filewatch_init ():
    load_config()
    load_datastore()

    filewatcher = Observer()
    filewatcher.schedule(ModifiedHandler(), "./", recursive=False)
    filewatcher.start()
    return filewatcher


class Status (enum.Enum):
    UNKNOWN = enum.auto()
    CONNECTING = enum.auto()
    CONNECTED = enum.auto()
    DISCONNECTED = enum.auto()
    IDLE = enum.auto()
    ERROR = enum.auto()
    EXIT = enum.auto()


CONN_FMT = "<t:{timestamp}:S>"
def update_discord (status: Status = Status.UNKNOWN, error: str = ""):
    global DATASTORE
    print(f"Send status: {status.name}")
    ds_embed = {
        "title": "CSO Re-Server Master Server Tracker",
        "fields": [
            {
                "name": "Name",
                "value": "",
                "inline": True
            },
            {
                "name": "Last Time Connected",
                "value": "None",
                "inline": False
            },
            {
                "name": "Status",
                "value": "❓Unknown",
                "inline": False
            },
        ],
    }
    ds_fields = ds_embed["fields"]
    ds_field_name = ds_fields[0]
    ds_field_timestamp = ds_fields[1]
    ds_field_status = ds_fields[2]

    ms_name = environ["CSORSE_MASTERSERVER_NAME"]
    if not ms_name:
        ds_fields.remove(ds_field_name)
    else:
        ds_field_name["name"] = ms_name

    ts_last: int = int(DATASTORE.get("ms_conn_last_time", 0))

    match status:
        case Status.IDLE:
            ds_field_status["value"] = "🟡Idle"
        case Status.CONNECTING:
            ds_field_status["value"] = "🔄Connecting"
        case Status.CONNECTED:
            ds_field_status["value"] = "🟢Connected"
            ts_last = int(time.time() // 1)
        case Status.DISCONNECTED:
            ds_field_status["value"] = "🔴Disconnected"
        case Status.ERROR:
            ds_field_status["value"] = f"⁉️Error\n{error}"
        case Status.EXIT:
            ds_field_status["value"] = f"💤Sleep"
        case _:
            ds_field_status["value"] = "❓Unknown"

    if ts_last:
        ds_field_timestamp["value"] = CONN_FMT.format(timestamp=ts_last)
        DATASTORE["ms_conn_last_time"] = ts_last

    if status not in [Status.IDLE, Status.CONNECTING, Status.CONNECTED, Status.EXIT]:
        ts_next = int(time.time() // 1) + int(environ["CSORSE_MASTERSERVER_UPDATE_INTERVAL"])
        ds_fields.append({
            "name": "Next Retry Time",
            "value": f"<t:{ts_next}:R>",
            "inline": False
        })

    data=json.dumps({"embeds": [ds_embed]})
    headers = {
        "Content-Type": "application/json",
    }
    wh_url = environ["DISCORD_WEBHOOK_URL"]

    msg_id = DATASTORE.get("ms_discord_msg_id")
    if msg_id:
        r = requests.get(f"{wh_url}/messages/{msg_id}")
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as err:
            msg_id = None

    if msg_id:
        r = requests.patch(
            f"{wh_url}/messages/{msg_id}",
            headers=headers,
            data=data,
        )
    else:
        r = requests.post(
            wh_url,
            headers=headers,
            params={
                "wait": "true"
            },
            data=data,
        )
    # Export the data for use in future steps
    return r.json()


class SockRecvForever (threading.Thread):
    def __init__ (self, s: socket.socket, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sock = s
        self.exc: BaseException = None

    def run (self):
        while s := self.sock:
            try:
                s.setblocking(True)
                s.recv(DEFAULT_BUFFER_SIZE)
            except (BaseException,) as exc:
                self.exc = exc
                break


def main ():
    global DATASTORE, FILEWATCHER
    signal.signal(signal.SIGINT, signal.default_int_handler)
    FILEWATCHER = filewatch_init()

    def send_status_and_store (*args, **kwargs):
        response = update_discord(*args, **kwargs)
        DATASTORE["ms_discord_msg_id"] = response["id"]
        save_datastore()

    def sigterm_handler (*args, **kwargs):
        send_status_and_store(Status.EXIT)
        return signal.default_int_handler(*args, **kwargs)
    signal.signal(signal.SIGTERM, sigterm_handler)

    send_status_and_store()

    timeout = int(environ["CSORSE_MASTERSERVER_TIMEOUT"])
    sock_thread: SockRecvForever = None

    try:
        while True:
            interval = float(environ["CSORSE_MASTERSERVER_UPDATE_INTERVAL"])
            if int(environ["CSORSE_MASTERSERVER_SLEEP_MODE"]):
                print("Sleep mode is active")
                interval = float(environ["CSORSE_MASTERSERVER_SLEEP_UPDATE_INTERVAL"])
                send_status_and_store(Status.EXIT)
            else:
                send_status_and_store(Status.IDLE)
                print(f"Checking server status: {environ['CSORSE_MASTERSERVER_IP']} {environ['CSORSE_MASTERSERVER_PORT']}")
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    try:
                        send_status_and_store(Status.CONNECTING)
                        s.settimeout(timeout)
                        s.connect((
                            environ["CSORSE_MASTERSERVER_IP"],
                            int(environ["CSORSE_MASTERSERVER_PORT"])
                        ))
                        s.settimeout(timeout)
                        data = s.recv(1024)
                        if data not in [b"~SERVERCONNECTED\n", b"~SERVERCONNECTED\n\x00"]:
                            raise RuntimeError("Not a master server instance")
                        send_status_and_store(Status.CONNECTED)
                        if not sock_thread:
                            sock_thread = SockRecvForever(s, daemon=True)
                            sock_thread.start()
                        sock_thread.join()
                        if sock_thread.exc:
                            raise sock_thread.exc
                    except (ConnectionRefusedError, TimeoutError, ConnectionResetError):
                        send_status_and_store(Status.DISCONNECTED)
                    except Exception as error:
                        send_status_and_store(Status.ERROR, f"{type(error).__name__}: {error}")
                    finally:
                        sock_thread = None
                save_datastore()
            print(f"Sleep for {interval} second(s)")
            print(f"==============================")
            time.sleep(interval)
    finally:
        send_status_and_store(Status.EXIT)

if __name__ == "__main__":
    main()
