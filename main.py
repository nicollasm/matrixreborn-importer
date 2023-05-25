# Importação das bibliotecas necessárias
import logging
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import exc, MetaData, Table
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
from tkinter import Button, Entry, Label, StringVar, Tk, filedialog, messagebox

# Configuração do logger
logging.basicConfig(level=logging.INFO)

# Definição de constantes globais
CHUNKSIZE = 50000  # Número de linhas a serem lidas por vez
DELIMITER = ';'  # Delimitador dos campos no arquivo CSV
DB_ENGINE = 'postgresql+psycopg2'  # Engine do SQLAlchemy para o PostgreSQL

class Database:
    """Classe que representa a conexão com o banco de dados."""

    def __init__(self, dbname, user, password, host):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.engine = None

    def test_connection(self):
        """
        Testa a conexão com o banco de dados. Emite um log e levanta uma exceção em caso de falha.
        """
        self.engine = create_engine(f'{DB_ENGINE}://{self.user}:{self.password}@{self.host}/{self.dbname}')
        try:
            self.engine.connect()
            logging.info("Conexão bem-sucedida.")
        except exc.DBAPIError as e:
            logging.error("Não foi possível conectar ao banco de dados.")
            raise

    def drop_table(self, table_name):
        """
        Apaga uma tabela do banco de dados. Emite um log e levanta uma exceção em caso de falha.
        """
        if not table_name:
            logging.error("O nome da tabela não é válido.")
            raise ValueError("O nome da tabela não é válido.")

        try:
            with self.engine.begin() as connection:
                metadata = MetaData()
                table = Table(table_name, metadata, autoload_with=self.engine)
                table.drop(self.engine)
                logging.info(f"Tabela {table_name} apagada.")
        except exc.NoSuchTableError:
            logging.info(f"A tabela {table_name} não existe.")
        except Exception as e:
            logging.error(f"Ocorreu um erro inesperado durante a exclusão da tabela: {e}")
            raise

class Importer:
    """Classe que representa o importador de dados."""

    def __init__(self, db, file_path):
        self.db = db
        self.file_path = file_path

    def import_data(self, table_name):
        """
        Importa dados de um arquivo CSV para o banco de dados. Emite um log e levanta uma exceção em caso de falha.
        """
        if not table_name:
            logging.error("O nome da tabela não é válido.")
            raise ValueError("O nome da tabela não é válido.")

        total_rows = 0
        try:
            for chunk in pd.read_csv(self.file_path, chunksize=CHUNKSIZE, delimiter=DELIMITER, engine='python',
                                     dtype={13: 'str'}):
                chunk.to_sql(table_name, self.db.engine, if_exists='append', index=False)
                total_rows += len(chunk)
            logging.info(f"Dados importados com sucesso. Total de linhas: {total_rows}.")
        except Exception as e:
            logging.error(f"Ocorreu um erro inesperado durante a importação: {e}")
            raise

class App:
    """Classe que representa a interface do usuário."""

    def __init__(self, window, window_title):
        self.window = window
        self.window.title(window_title)
        self.db = None

        self.file_path = StringVar()

        Label(self.window, text="Arquivo CSV:").pack()
        self.browse_button = Button(self.window, text="Browse", command=self.browse_file)
        self.browse_button.pack()

        Label(self.window, text="Nome da tabela:").pack()
        self.table_entry = Entry(self.window)
        self.table_entry.pack()

        Label(self.window, text="Nome do banco de dados:").pack()
        self.dbname_entry = Entry(self.window)
        self.dbname_entry.pack()

        Label(self.window, text="Usuário:").pack()
        self.user_entry = Entry(self.window)
        self.user_entry.pack()

        Label(self.window, text="Senha:").pack()
        self.password_entry = Entry(self.window, show="*")
        self.password_entry.pack()

        Label(self.window, text="Host:").pack()
        self.host_entry = Entry(self.window)
        self.host_entry.pack()

        self.test_button = Button(self.window, text="Testar Conexão", command=self.test_connection)
        self.test_button.pack()

        self.import_button = Button(self.window, text="Importar", command=self.import_data)
        self.import_button.pack()

    def browse_file(self):
        """Abre a janela de seleção de arquivo."""
        self.file_path.set(filedialog.askopenfilename())

    def test_connection(self):
        """Testa a conexão com o banco de dados e exibe uma mensagem de sucesso ou erro."""
        dbname = self.dbname_entry.get()
        user = self.user_entry.get()
        password = self.password_entry.get()
        host = self.host_entry.get()

        if not dbname or not user or not password or not host:
            messagebox.showerror("Testar Conexão", "Todos os campos devem ser preenchidos.")
            return

        self.db = Database(dbname, user, password, host)
        try:
            self.db.test_connection()
            messagebox.showinfo("Testar Conexão", "Conexão bem-sucedida!")
        except Exception as e:
            messagebox.showerror("Testar Conexão", str(e))

    def import_data(self):
        """Importa dados para o banco de dados e exibe uma mensagem de sucesso ou erro."""
        table_name = self.table_entry.get()
        if not table_name:
            messagebox.showerror("Importar", "O nome da tabela deve ser preenchido.")
            return
        if not self.db:
            messagebox.showerror("Importar", "O banco de dados não está configurado. Teste a conexão primeiro.")
            return
        self.db.drop_table(table_name)
        try:
            importer = Importer(self.db, self.file_path.get())
            importer.import_data(table_name)
            messagebox.showinfo("Importar", "Importação bem-sucedida!")
        except Exception as e:
            messagebox.showerror("Importar", str(e))


if __name__ == '__main__':
    root = Tk()
    app = App(root, "Aplicativo de Importação CSV")
    root.mainloop()
