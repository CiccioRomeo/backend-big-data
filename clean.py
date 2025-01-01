import os
import shutil

# Percorso della directory temporanea
temp_dir = r"C:\\Users\\romeo\\AppData\\Local\\Temp"

# Cerca ed elimina le cartelle che iniziano con "blockmgr-"
for folder in os.listdir(temp_dir):
    if folder.startswith("blockmgr-"):
        folder_path = os.path.join(temp_dir, folder)
        try:
            shutil.rmtree(folder_path)
            print(f"Eliminata cartella: {folder_path}")
        except Exception as e:
            print(f"Errore nell'eliminare {folder_path}: {e}")

print("Pulizia completata!")
