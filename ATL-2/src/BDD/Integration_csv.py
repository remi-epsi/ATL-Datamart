import pandas as pd
import psycopg2

# Création d'un DataFrame de test
csv_file_path = r"C:\Users\Rémi\Downloads\taxi+_zone_lookup.csv"
df = pd.read_csv(csv_file_path, sep=',')

# Création d'un dictionnaire pour stocker les correspondances entre les valeurs et les IDs
id_mapping = {}
id_mapping_service_zone = {}

# Fonction pour attribuer un ID unique
def assign_unique_id_location(value):
    if value not in id_mapping_service_zone:
        id_mapping_service_zone[value] = len(id_mapping_service_zone)
    return id_mapping_service_zone[value]

def assign_unique_id_zone(value):
    if value not in id_mapping:
        id_mapping[value] = len(id_mapping)
    return id_mapping[value]

def assign_unique_id_service_zone(value):
    if value not in id_mapping_service_zone:
        id_mapping_service_zone[value] = len(id_mapping_service_zone)
    return id_mapping_service_zone[value]

# Application de la fonction pour obtenir les IDs
df['ZoneID'] = df['Zone'].apply(assign_unique_id_zone)
df['Service_zoneID'] = df['service_zone'].apply(assign_unique_id_service_zone)
df['BoroughID'] = df['Borough'].apply(assign_unique_id_location)

df_location = df[['LocationID', 'BoroughID', 'ZoneID']]
df_location.to_csv(r'C:\Users\Rémi\Downloads\location.csv', index=False)

df_borough = df[['BoroughID' , 'Borough']]
df_borough = df_borough.drop_duplicates(subset='BoroughID', keep='first')
df_borough.to_csv(r'C:\Users\Rémi\Downloads\borough.csv', index=False)

df_zone = df[['Zone', 'ZoneID','Service_zoneID']].copy()
df_zone = df_zone.drop_duplicates(subset='ZoneID', keep='first')
df_zone = df_zone[['ZoneID', 'Zone','Service_zoneID']]
df_zone.to_csv(r'C:\Users\Rémi\Downloads\zone.csv', index=False)

df_service_zone = df[['service_zone', 'Service_zoneID']].copy()
df_service_zone = df_service_zone.drop_duplicates(subset='Service_zoneID', keep='first')
df_service_zone = df_service_zone[['Service_zoneID', 'service_zone']]
df_service_zone.to_csv(r'C:\Users\Rémi\Downloads\service_zone.csv', index=False)



#print(df_location)


# Affichage du DataFrame résultant


