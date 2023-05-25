    # Importar as bibliotecas necessárias
    import logging
    import pandas as pd
    from sqlalchemy import create_engine, exc
    from sqlalchemy import MetaData, Table
    from tkinter import filedialog, messagebox
    from tkinter import Tk, Label, Button, StringVar, Entry

    # Definir constantes para valores que podem precisar de alteração no futuro
    CHUNKSIZE = 10 ** 5
    DELIMITER = ';'
    DB_ENGINE = 'c'

    # Configurar o logger
    logging.basicConfig(level=logging.INFO)


    class Database:
        """Classe que representa a conexão com o banco de dados."""

        def __init__(self, dbname, user, password, host):
            self.engine = self.create_engine(dbname, user, password, host)

        def create_engine(self, dbname, user, password, host):
            """Cria e retorna um engine do SQLAlchemy."""
            engine_url = f'postgresql://{user}:{password}@{host}/{dbname}'
            return create_engine(engine_url)

        def test_connection(self):
            """
            Testa a conexão com o banco de dados. Emite um log e levanta uma exceção em caso de falha.
            """
            try:
                with self.engine.connect() as connection:
                    pass
            except exc.OperationalError:
                logging.error("Não foi possível conectar ao banco de dados. Verifique suas credenciais e tente novamente.")
                raise
            except Exception as e:
                logging.error(f"Ocorreu um erro inesperado: {e}")
                raise

        def drop_table(self, table_name):
            """
            Apaga uma tabela no banco de dados. Emite um log e levanta uma exceção em caso de falha.
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
                for chunk in pd.read_csv(self.file_path, chunksize=CHUNKSIZE, delimiter=DELIMITER, engine=DB_ENGINE,
                                         dtype={13: 'str'}):
                    chunk.to_sql(table_name, self.db.engine, if_exists='append', index=False)
                    total_rows += len(chunk)
                    logging.info(f"{total_rows} linhas importadas.")
            except FileNotFoundError:
                logging.error("Arquivo não encontrado. Verifique o caminho do seu arquivo e tente novamente.")
                raise
            except Exception as e:
                logging.error(f"Ocorreu um erro inesperado durante a importação dos dados: {e}")
                raise


    class App:
        """Classe que representa a aplicação principal."""

        def __init__(self, window, window_title):
            self.window = window
            self.window.title(window_title)
            self.create_widgets()
            self.db = None

        def create_widgets(self):
            """Cria os widgets da interface do usuário."""
            self.create_file_widgets()
            self.create_table_widgets()
            self.create_database_widgets()

        def create_file_widgets(self):
            """Cria os widgets relacionados ao arquivo."""
            self.file_path = StringVar()
            self.browse_button = Button(self.window, text="Navegar", command=self.browse_file)
            self.browse_button.pack()

        def create_table_widgets(self):
            """Cria os widgets relacionados à tabela."""
            self.table_name = StringVar()
            self.table_label = Label(self.window, text="Nome da Tabela")
            self.table_label.pack()
            self.table_entry = Entry(self.window, textvariable=self.table_name)
            self.table_entry.pack()
            self.import_button = Button(self.window, text="Importar", command=self.import_data)
            self.import_button.pack()

        def create_database_widgets(self):
            """Cria os widgets relacionados ao banco de dados."""
            self.dbname_label = Label(self.window, text="Nome do Banco de Dados")
            self.dbname_label.pack()
            self.dbname_entry = Entry(self.window)
            self.dbname_entry.pack()
            self.user_label = Label(self.window, text="Nome do Usuário")
            self.user_label.pack()
            self.user_entry = Entry(self.window)
            self.user_entry.pack()
            self.password_label = Label(self.window, text="Senha")
            self.password_label.pack()
            self.password_entry = Entry(self.window, show='*')
            self.password_entry.pack()
            self.host_label = Label(self.window, text="Host")
            self.host_label.pack()
            self.host_entry = Entry(self.window)
            self.host_entry.pack()
            self.test_button = Button(self.window, text="Testar Conexão", command=self.test_connection)
            self.test_button.pack()

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
