import socket
import tkinter as tk
import configparser


class SocketClient:
    def __init__(self, host, port, socket_client_GUI):
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((host, port))
        self.socket_client_GUI = socket_client_GUI
        self.socket_client_GUI._log(f"成功連線 SQL on {host}:{port}...")

    def send(self, msg):
        msg = msg.replace("\n", "")
        self.client_socket.send(msg.encode())
        self.socket_client_GUI._log("ME: " + msg)
        res = self.client_socket.recv(2048).decode()
        self.socket_client_GUI._log("SEVER: " + res)


class SocketClientGUI:
    def __init__(self):
        self._root = tk.Tk()
        self._root.title("Socket Programming GUI")

        self._input_text = tk.Text(self._root, height=2, width=130, state=tk.NORMAL, font=20)
        self._input_text.grid(row=0, column=0, columnspan=7, padx=10, pady=10)

        self._log_text = tk.Text(self._root, height=10, width=130, state=tk.DISABLED, font=20)
        self._log_text.grid(row=1, column=0, columnspan=7, padx=10, pady=10)

        self._select_button = tk.Button(self._root, text="Select", command=self._select_template, height=2, width=15)
        self._select_button.grid(row=2, column=0, columnspan=2, pady=10)
        self._insert_button = tk.Button(self._root, text="Insert", command=self._insert_template, height=2, width=15)
        self._insert_button.grid(row=2, column=1, columnspan=2, pady=10)
        self._update_button = tk.Button(self._root, text="Update", command=self._update_template, height=2, width=15)
        self._update_button.grid(row=2, column=2, columnspan=2, pady=10)
        self._delete_button = tk.Button(self._root, text="Delete", command=self._delete_template, height=2, width=15)
        self._delete_button.grid(row=2, column=3, columnspan=2, pady=10)

        self._submit_button = tk.Button(self._root, text="Submit", command=self._on_submit, height=2, width=10)
        self._submit_button.grid(row=2, column=5, columnspan=2, pady=15)

        self._socket_start()
        self._root.mainloop()

    def _socket_start(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        host = config['socket']['host']
        port = int(config['socket']['port'])
        self.socket_client = SocketClient(host, port, self)

    def _output(self, string):
        self._input_text.delete('1.0', tk.END)
        self._input_text.insert(tk.END, f"{string}\n")
        self._input_text.yview(tk.END)

    def _log(self, string):
        self._log_text.config(state=tk.NORMAL)
        self._log_text.insert(tk.END, f"{string}\n")
        self._log_text.config(state=tk.DISABLED)
        self._log_text.yview(tk.END)

    def _select_template(self):
        self._output(
            """SELECT (`id`, `account`, `password`, `time`) FROM `sql_sever` WHERE (`id`='2', `password`='ADMIN') ORDER BY `time` DESC""")

    def _insert_template(self):
        self._output(
            """INSERT INTO `sql_sever` (`id`, `account`, `password`, `time`) VALUES ('0','ROOT','ADMIN','2023')""")

    def _update_template(self):
        self._output(
            """UPDATE `sql_sever` SET `id`='2',`account`='ROOT2',`password`='ADMIN2',`time`='2021' WHERE (`id`='2', `password`='ADMIN')""")

    def _delete_template(self):
        self._output("""DELETE FROM `sql_sever` WHERE (`id`='2', `password`='ADMIN2')""")

    def _on_submit(self):
        try:
            self.socket_client.send(self._input_text.get("1.0", "end-1c"))
        except Exception as e:
            self._log("連線已被您主機上的軟體中止。")


if __name__ == "__main__":
    socket_sever_GUI = SocketClientGUI()
