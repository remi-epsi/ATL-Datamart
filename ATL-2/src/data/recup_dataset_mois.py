import datetime
import requests
import os

'''

Objectif : Récuperer les fichiers parquet du dernier mois depuis une API de New York.


'''

def main():
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    date_actuelle = datetime.date.today()
    annee_actuelle = date_actuelle.year
    dernier_mois = date_actuelle.month-3

    # Générer l'URL pour le dernier mois
    nom_fichier = f"yellow_tripdata_{annee_actuelle}-{dernier_mois:02d}.parquet"
    if os.path.exists(f"data\\raw\\{nom_fichier}"):

        print("Le fichier existe déjà")
        
    else:
        
        url = base_url + nom_fichier
        response = requests.get(url)

        if response.status_code == 200:
            #Télécharger les fichiers sur le PC local
            with open("data\\raw\\"+nom_fichier, "wb") as f:
                f.write(response.content)
            print("Téléchargement du fichier terminé avec succès.")
        else:
            print("Erreur lors du téléchargement. Statut de la réponse :", response.status_code)

if __name__ == "__main__":
     main()