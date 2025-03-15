import tkinter as tk
from tkinter import messagebox
from shared.constants import SERVER_HOST, SERVER_PORT
from shared.utils import send_data, receive_data
import socket

class MainWindow:
    def __init__(self, root):
        self.root = root
        self.root.title("DBMS Client")
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((SERVER_HOST, SERVER_PORT))
        
        self.create_widgets()
    
    def create_widgets(self):
        # Create Database Section
        tk.Label(self.root, text="Create Database").grid(row=0, column=0, padx=10, pady=10)
        self.db_name_entry = tk.Entry(self.root)
        self.db_name_entry.grid(row=0, column=1, padx=10, pady=10)
        tk.Button(self.root, text="Create", command=self.create_database).grid(row=0, column=2, padx=10, pady=10)
        
        # Drop Database Section
        tk.Label(self.root, text="Drop Database").grid(row=1, column=0, padx=10, pady=10)
        self.drop_db_name_entry = tk.Entry(self.root)
        self.drop_db_name_entry.grid(row=1, column=1, padx=10, pady=10)
        tk.Button(self.root, text="Drop", command=self.drop_database).grid(row=1, column=2, padx=10, pady=10)
        
        # Create Table Section
        tk.Label(self.root, text="Create Table").grid(row=2, column=0, padx=10, pady=10)
        self.create_table_db_entry = tk.Entry(self.root)
        self.create_table_db_entry.grid(row=2, column=1, padx=10, pady=10)
        tk.Label(self.root, text="Table Name").grid(row=2, column=2, padx=10, pady=10)
        self.create_table_name_entry = tk.Entry(self.root)
        self.create_table_name_entry.grid(row=2, column=3, padx=10, pady=10)
        tk.Label(self.root, text="Columns (col1:type1 col2:type2)").grid(row=2, column=4, padx=10, pady=10)
        self.create_table_columns_entry = tk.Entry(self.root)
        self.create_table_columns_entry.grid(row=2, column=5, padx=10, pady=10)
        tk.Button(self.root, text="Create", command=self.create_table).grid(row=2, column=6, padx=10, pady=10)
        
        # Drop Table Section
        tk.Label(self.root, text="Drop Table").grid(row=3, column=0, padx=10, pady=10)
        self.drop_table_db_entry = tk.Entry(self.root)
        self.drop_table_db_entry.grid(row=3, column=1, padx=10, pady=10)
        tk.Label(self.root, text="Table Name").grid(row=3, column=2, padx=10, pady=10)
        self.drop_table_name_entry = tk.Entry(self.root)
        self.drop_table_name_entry.grid(row=3, column=3, padx=10, pady=10)
        tk.Button(self.root, text="Drop", command=self.drop_table).grid(row=3, column=4, padx=10, pady=10)
    
    def create_database(self):
        db_name = self.db_name_entry.get()
        if not db_name:
            messagebox.showerror("Error", "Database name cannot be empty.")
            return
        send_data(self.sock, {'action': 'create_database', 'db_name': db_name})
        response = receive_data(self.sock)
        messagebox.showinfo("Response", response['response'])
    
    def drop_database(self):
        db_name = self.drop_db_name_entry.get()
        if not db_name:
            messagebox.showerror("Error", "Database name cannot be empty.")
            return
        send_data(self.sock, {'action': 'drop_database', 'db_name': db_name})
        response = receive_data(self.sock)
        messagebox.showinfo("Response", response['response'])
    
    def create_table(self):
        db_name = self.create_table_db_entry.get()
        table_name = self.create_table_name_entry.get()
        columns = self.create_table_columns_entry.get()
        
        if not db_name or not table_name or not columns:
            messagebox.showerror("Error", "All fields are required.")
            return
        
        # Parse columns into a dictionary
        columns_dict = {}
        for col_def in columns.split():
            col_name, col_type = col_def.split(':')
            columns_dict[col_name] = {'type': col_type}
        
        send_data(self.sock, {'action': 'create_table', 'db_name': db_name, 'table_name': table_name, 'columns': columns_dict})
        response = receive_data(self.sock)
        messagebox.showinfo("Response", response['response'])
    
    def drop_table(self):
        db_name = self.drop_table_db_entry.get()
        table_name = self.drop_table_name_entry.get()
        
        if not db_name or not table_name:
            messagebox.showerror("Error", "All fields are required.")
            return
        
        send_data(self.sock, {'action': 'drop_table', 'db_name': db_name, 'table_name': table_name})
        response = receive_data(self.sock)
        messagebox.showinfo("Response", response['response'])
    
    def close(self):
        self.sock.close()

def run_gui():
    root = tk.Tk()
    app = MainWindow(root)
    root.mainloop()
    app.close()

if __name__ == "__main__":
    run_gui()
