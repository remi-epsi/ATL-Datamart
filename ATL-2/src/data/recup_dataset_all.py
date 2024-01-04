import requests

'''

Objectif : Récuperer les fichiers parquet de 2018 à aujourd'hui depuis une API de New York.


'''

def main():
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    urls_yellow_tripdata = []
    annee_debut = 2018
    annee_actuelle = 2023
    mois_actuel = 1

    # Générer les URL pour chaque année et mois depuis 2018 jusqu'à l'année et au mois actuels
    for annee in range(annee_debut, annee_actuelle + 1):
        for mois in range(1, 13):  
            
            nom_fichier = f"yellow_tripdata_{annee}-{mois:02d}.parquet"
            response = requests.get(base_url + nom_fichier)
            
            #Télécharger les fichiers sur le PC local
            if response.status_code == 200:
                with open("data\\raw\\"+nom_fichier, "wb") as f:
                    f.write(response.content)
            print("Téléchargement terminé avec succès.")
    else:
                print("Erreur lors du téléchargement. Statut de la réponse :", response.status_code)

if __name__ == "__main__":
     main()
