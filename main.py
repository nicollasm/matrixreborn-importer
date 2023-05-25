import logging
import pandas as pd
from sqlalchemy import create_engine, exc
from sqlalchemy import MetaData, Table
from tkinter import filedialog, messagebox
from tkinter import Tk, Label, Button, StringVar, Entry

# Configurar o logger
logging.basicConfig(level=logging.INFO)


class Database:
    def __init__(self, dbname, user, password, host):
        self.engine = self.create_engine(dbname, user, password, host)

    def create_engine(self, dbname, user, password, host):
        engine_url = f'postgresql://{user}:{password}@{host}/{dbname}'
        return create_engine(engine_url)

    def test_connection(self):
        try:
            with self.engine.connect() as connection:
                pass
        except exc.OperationalError:
            logging.error("Could not connect to the database. Please check your credentials and try again.")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            raise

    def drop_table(self, table_name):
        try:
            with self.engine.begin() as connection:
                metadata = MetaData()
                table = Table(table_name, metadata, autoload_with=self.engine)
                table.drop(self.engine)
                logging.info(f"Table {table_name} dropped.")
        except exc.NoSuchTableError:
            logging.info(f"Table {table_name} doesn't exist.")
        except Exception as e:
            logging.error(f"An unexpected error occurred during table drop: {e}")
            raise


class Importer:
    def __init__(self, db, file_path):
        self.db = db
        self.file_path = file_path

    def import_data(self, table_name):
        total_rows = 0
        try:
            for chunk in pd.read_csv(self.file_path, chunksize=10 ** 5, delimiter=';', engine='c', dtype={13: 'str'}):
                chunk.to_sql(table_name, self.db.engine, if_exists='append', index=False)
                total_rows += len(chunk)
                logging.info(f"{total_rows} rows imported.")
        except FileNotFoundError:
            logging.error("File not found. Please check your file path and try again.")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred during data import: {e}")
            raise


class App:
    def __init__(self, window, window_title):
        self.window = window
        self.window.title(window_title)
        self.create_widgets()
        self.db = None

    def create_widgets(self):
        self.create_file_widgets()
        self.create_table_widgets()
        self.create_database_widgets()

    def create_file_widgets(self):
        self.file_path = StringVar()
        self.browse_button = Button(self.window, text="Browse", command=self.browse_file)
        self.browse_button.pack()

    def create_table_widgets(self):
        self.table_name = StringVar()
        self.table_label = Label(self.window, text="Table Name")
        self.table_label.pack()
        self.table_entry = Entry(self.window, textvariable=self.table_name)
        self.table_entry.pack()
        self.import_button = Button(self.window, text="Import", command=self.import_data)
        self.import_button.pack()

    def create_database_widgets(self):
        self.dbname_label = Label(self.window, text="Database Name")
        self.dbname_label.pack()
        self.dbname_entry = Entry(self.window)
        self.dbname_entry.pack()
        self.user_label = Label(self.window, text="User Name")
        self.user_label.pack()
        self.user_entry = Entry(self.window)
        self.user_entry.pack()
        self.password_label = Label(self.window, text="Password")
        self.password_label.pack()
        self.password_entry = Entry(self.window, show='*')
        self.password_entry.pack()
        self.host_label = Label(self.window, text="Host")
        self.host_label.pack()
        self.host_entry = Entry(self.window)
        self.host_entry.pack()
        self.test_button = Button(self.window, text="Test Connection", command=self.test_connection)
        self.test_button.pack()

    def browse_file(self):
        self.file_path.set(filedialog.askopenfilename())

    def test_connection(self):
        dbname = self.dbname_entry.get()
        user = self.user_entry.get()
        password = self.password_entry.get()
        host = self.host_entry.get()

        if not dbname or not user or not password or not host:
            messagebox.showerror("Test Connection", "All fields must be filled.")
            return

        self.db = Database(dbname, user, password, host)
        try:
            self.db.test_connection()
            messagebox.showinfo("Test Connection", "Connection successful!")
        except Exception as e:
            messagebox.showerror("Test Connection", str(e))

    def import_data(self):
        table_name = self.table_entry.get()
        if not table_name:
            messagebox.showerror("Import", "Table name must be filled.")
            return
        if not self.db:
            messagebox.showerror("Import", "Database is not configured. Test the connection first.")
            return
        self.db.drop_table(table_name)
        importer = Importer(self.db, self.file_path.get())
        try:
            importer.import_data(table_name)
            messagebox.showinfo("Import", "Data import successful!")
        except Exception as e:
            messagebox.showerror("Import", str(e))

    def run(self):
        self.window.mainloop()


if __name__ == "__main__":
    App(Tk(), "Data Importer").run()
