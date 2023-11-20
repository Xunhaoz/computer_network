import os
import re
import socket
import logging
import configparser

import pandas as pd


class SocketSever:
    def __init__(self, host, port, backlog):
        self.host = host
        self.port = port
        self.backlog = backlog
        self.server_socket = None

        self._init_host()
        self._start_connection()

    def _init_host(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(self.backlog)
        self.server_socket.setblocking(False)
        logging.info(f"Attempting to start SQL on {self.host}:{self.port}...")
        logging.info(f"Max Connection {self.backlog}")

    def _start_connection(self):
        clients = []
        while True:
            try:
                client, addr = self.server_socket.accept()
                logging.info(f'client ip：{addr[0]}, client port：{addr[1]}')
                client.setblocking(False)
                clients.append(client)
            except:
                pass

            for client in clients:
                try:
                    command = client.recv(2048).decode('utf8')
                    logging.info(f'client command: {command}')
                    reply = self._handle_sql(command)
                    if "sql sever error" in reply:
                        reply = "SEVER ERROR, CONNECTION WILL BE CLOSED NOW."
                        logging.info(f'sever reply: {reply}')
                        client.send(reply.encode())
                        client.close()
                    else:
                        client.send(reply.encode())
                except:
                    pass

    def _handle_sql(self, command):
        command = self.clean_sep_char(command)
        try:
            if "SELECT" in command:
                res = self._select(command)
            elif "UPDATE" in command:
                res = self._update(command)
            elif "INSERT" in command:
                res = self._insert(command)
            elif "DELETE" in command:
                res = self._delete(command)
            else:
                res = "sql sever error"
        except Exception as e:
            res = f"sql sever error: {e}"
        return res

    def _insert(self, statement):
        rule = (r"INSERTINTO" + self.reg_get("SCHEME", "[^\(]+") + "\(" +
                self.reg_get("COLS", "([^\)]+)") + "\)VALUES\(" + self.reg_get("VALUES", "([^\)]+)") + "\)")
        match_dict = self.match_all(rule, statement)

        if self.not_found(match_dict['SCHEME']):
            df = pd.DataFrame({col: [] for col in match_dict['COLS'].split(',')})
            res = "DataFrame does not exist, so sql create a new one."
        else:
            df = self.df_read(match_dict['SCHEME'])
            res = "Insert Successful"
        new_row = {col: val for col, val in zip(match_dict['COLS'].split(','), match_dict['VALUES'].split(','))}
        df = df._append(new_row, ignore_index=True)
        self.df_save(df, match_dict['SCHEME'])
        return res

    def _select(self, statement):
        rule = (r"SELECT\(" + self.reg_get("COLS", "([^)]+)") + "\)FROM" +
                self.reg_get("SCHEME", "(.*)") + "WHERE\(" + self.reg_get("WHERE", "([^)]+)") +
                "\)ORDERBY" + self.reg_get("ORDERBY", "(.*)") + "(DESC|ASC)")
        match_dict = self.match_all(rule, statement)

        if self.not_found(match_dict['SCHEME']): return self.not_found(match_dict['SCHEME'])


        df = self.df_read(match_dict['SCHEME'])

        if "WHERE" in match_dict:
            for col, val in self.s_comma_equal(match_dict['WHERE']):
                df = df[df[col] == val]

        if "ORDERBY" in match_dict:
            if 'DESC' in statement:
                df = df.sort_values(match_dict['ORDERBY'], ascending=False)
            else:
                df = df.sort_values(match_dict['ORDERBY'])

        df = df[match_dict['COLS'].split(',')]
        return df.to_json()

    def _update(self, statement):
        rule = (r"UPDATE" + self.reg_get("SCHEME", "(.*)") + "SET" + self.reg_get("COLS", "(.*)") +
                "WHERE\(" + self.reg_get("WHERE", "([^)]+)") + "\)")
        match_dict = self.match_all(rule, statement)

        if self.not_found(match_dict['SCHEME']): return self.not_found(match_dict['SCHEME'])

        df = self.df_read(match_dict['SCHEME'])

        condition = (df.iloc[:, 0] == df.iloc[:, 0])

        if "WHERE" in match_dict:
            for col, val in self.s_comma_equal(match_dict['WHERE']):
                condition = condition & (df[col] == val)

        for col, val in self.s_comma_equal(match_dict['COLS']):
            df.loc[condition, col] = val

        self.df_save(df, match_dict['SCHEME'])

        return "Update Successful."

    def _delete(self, statement):
        rule = r"DELETEFROM" + self.reg_get("SCHEME", "(.*)") + "WHERE\(" + self.reg_get("WHERE", "(.*)") + "\)"
        match_dict = self.match_all(rule, statement)

        if self.not_found(match_dict['SCHEME']): return self.not_found(match_dict['SCHEME'])

        df = self.df_read(match_dict['SCHEME'])

        if 'WHERE' in match_dict:
            for col, val in self.s_comma_equal(match_dict['WHERE']):
                df = df[df[col] != val]
        else:
            df = df.drop(df.index)

        self.df_save(df, match_dict['SCHEME'])

        return "Delete successful."

    @staticmethod
    def match_all(r, s):
        reg = re.match(r, s)
        mat = reg and reg.group() == s
        if not mat: return None
        reg_dict = reg.groupdict()
        for k, v in reg_dict.items():
            if v is None: reg_dict[k] = ''
        return reg_dict

    @staticmethod
    def reg_get(x, s):
        return s if x is None else r'(?P<' + x + '>' + s + r')'

    @staticmethod
    def df_read(name):
        return pd.read_csv(name + ".csv").astype(str)

    @staticmethod
    def df_save(df, name):
        df.to_csv(name + ".csv", index=False)

    @staticmethod
    def not_found(name):
        return "" if os.path.exists(name + ".csv") else "DataFrame does not exist."

    @staticmethod
    def s_comma_equal(l):
        return [ll.split('=') for ll in l.split(',')]

    @staticmethod
    def clean_sep_char(s):
        replace_char = ["'", '"', " ", "`", "\n"]
        for c in replace_char: s = s.replace(c, "")
        return s.upper()


if __name__ == "__main__":
    logging.basicConfig(filename='sql.log', encoding='utf-8', level=logging.INFO)

    config = configparser.ConfigParser()
    config.read('config.ini')
    host = config['socket']['host']
    port = int(config['socket']['port'])
    backlog = int(config['socket']['backlog'])

    SocketSever(host, port, backlog)
