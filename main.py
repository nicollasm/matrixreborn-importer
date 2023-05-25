# Importação das bibliotecas necessárias
import logging
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import exc, MetaData, Table
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
from PyQt5 import QtWidgets
from PyQt5.QtWidgets import QFileDialog, QLabel, QLineEdit, QMessageBox, QPushButton, QVBoxLayout, QWidget, QComboBox
import re

# Configuração do logger para controle de eventos e erros
logging.basicConfig(level=logging.INFO)

# Definição de constantes globais
CHUNKSIZE = 50000  # Número de linhas a serem lidas por vez do arquivo CSV
DB_ENGINE = 'postgresql+psycopg2'  # Engine do SQLAlchemy para o PostgreSQL

def clean_phone_number(phone_number):
    """
    Limpa o número do telefone, removendo caracteres especiais e espaços em branco.
    """
    return ''.join(filter(str.isdigit, str(phone_number)))


class Database:
    """Classe que representa a conexão com o banco de dados."""

    def __init__(self, dbname, user, password, host):
        self.engine = create_engine(f'{DB_ENGINE}://{user}:{password}@{host}/{dbname}')

    def test_connection(self):
        """
        Testa a conexão com o banco de dados. Emite um log e levanta uma exceção em caso de falha.
        """
        try:
            self.engine.connect()
            logging.info("Conexão bem-sucedida.")
        except exc.DBAPIError as e:
            logging.error("Não foi possível conectar ao banco de dados.")
            raise


class Importer:
    """Classe que representa o importador de dados."""

    def __init__(self, db, file_path, delimiter):
        self.db = db
        self.file_path = file_path
        self.delimiter = delimiter

    def import_data(self, table_name, phone_columns):
        """
        Importa dados de um arquivo CSV para o banco de dados.
        """
        if not table_name:
            logging.error("O nome da tabela não é válido.")
            raise ValueError("O nome da tabela não é válido.")

        total_rows = 0
        try:
            for chunk in pd.read_csv(self.file_path, chunksize=CHUNKSIZE, delimiter=self.delimiter, engine='python',
                                     dtype={13: 'str'}):
                # Limpar colunas de telefone
                for column in phone_columns:
                    if column in chunk.columns:
                        chunk[column] = chunk[column].apply(clean_phone_number)

                chunk.to_sql(table_name, self.db.engine, if_exists='append', index=False)
                total_rows += len(chunk)

            logging.info(f"Dados importados com sucesso. Total de linhas: {total_rows}.")
        except pd.errors.ParserError as e:
            logging.error(f"Erro ao ler o arquivo CSV: {e}")
            raise
        except Exception as e:
            logging.error(f"Ocorreu um erro inesperado durante a importação: {e}")
            raise


class App:
    """Classe que representa a interface do usuário."""

    def __init__(self):
        self.window = QWidget()
        self.window.setWindowTitle("Aplicativo de Importação CSV")
        self.db = None
        self.layout = QVBoxLayout()

        self.file_path = QLineEdit()
        self.browse_button = QPushButton("Browse")
        self.browse_button.clicked.connect(self.browse_file)
        self.layout.addWidget(QLabel("Arquivo CSV:"))
        self.layout.addWidget(self.file_path)
        self.layout.addWidget(self.browse_button)

        self.table_entry = QLineEdit()
        self.layout.addWidget(QLabel("Nome da tabela:"))
        self.layout.addWidget(self.table_entry)

        self.dbname_entry = QLineEdit()
        self.layout.addWidget(QLabel("Nome do banco de dados:"))
        self.layout.addWidget(self.dbname_entry)

        self.user_entry = QLineEdit()
        self.layout.addWidget(QLabel("Usuário:"))
        self.layout.addWidget(self.user_entry)

        self.password_entry = QLineEdit()
        self.password_entry.setEchoMode(QLineEdit.Password)
        self.layout.addWidget(QLabel("Senha:"))
        self.layout.addWidget(self.password_entry)

        self.host_entry = QLineEdit()
        self.layout.addWidget(QLabel("Host:"))
        self.layout.addWidget(self.host_entry)

        self.delimiter_entry = QLineEdit()
        self.layout.addWidget(QLabel("Delimitador:"))
        self.layout.addWidget(self.delimiter_entry)

        self.test_button = QPushButton("Testar Conexão")
        self.test_button.clicked.connect(self.test_connection)
        self.layout.addWidget(self.test_button)

        self.import_button = QPushButton("Importar")
        self.import_button.clicked.connect(self.import_data)
        self.layout.addWidget(self.import_button)

        self.window.setLayout(self.layout)

    def browse_file(self):
        """Abre a janela de seleção de arquivo."""
        file_path, _ = QFileDialog.getOpenFileName()
        self.file_path.setText(file_path)

    def test_connection(self):
        """Testa a conexão com o banco de dados e exibe uma mensagem de sucesso ou erro."""
        dbname = self.dbname_entry.text()
        user = self.user_entry.text()
        password = self.password_entry.text()
        host = self.host_entry.text()

        if not dbname or not user or not password or not host:
            QMessageBox.critical(self.window, "Testar Conexão", "Todos os campos devem ser preenchidos.")
            return

        self.db = Database(dbname, user, password, host)
        try:
            self.db.test_connection()
            QMessageBox.information(self.window, "Testar Conexão", "Conexão bem-sucedida!")
        except Exception as e:
            QMessageBox.critical(self.window, "Testar Conexão", str(e))

    def import_data(self):
        """Importa dados para o banco de dados e exibe uma mensagem de sucesso ou erro."""
        table_name = self.table_entry.text()
        if not table_name:
            QMessageBox.critical(self.window, "Importar", "O nome da tabela deve ser preenchido.")
            return
        if not self.db:
            QMessageBox.critical(self.window, "Importar",
                                 "O banco de dados não está configurado. Teste a conexão primeiro.")
            return
        try:
            importer = Importer(self.db, self.file_path.text(), self.delimiter_entry.text())
            importer.import_data(table_name, ['telefone1', 'telefone2', 'telefone3'])
            QMessageBox.information(self.window, "Importar", "Dados importados com sucesso!")
        except Exception as e:
            QMessageBox.critical(self.window, "Importar", str(e))


def main():
    """Ponto de entrada principal do aplicativo."""
    app = QtWidgets.QApplication([])

    application = App()
    application.window.show()

    app.exec()


if __name__ == "__main__":
    main()
