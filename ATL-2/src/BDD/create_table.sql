--
-- Création des tables du modèle FLocon
--

CREATE TABLE Vendor (vendorid BIGINT PRIMARY KEY, name_vendor VARCHAR(50));
CREATE TABLE service_zone (service_zoneID BIGINT PRIMARY KEY, name_service_zone VARCHAR(50));
CREATE TABLE Zone (zoneID BIGINT PRIMARY KEY, name_zone VARCHAR(50), service_zoneID BIGINT, FOREIGN KEY (service_zoneID) REFERENCES service_zone(service_zoneID));
CREATE TABLE Location (LocationID BIGINT PRIMARY KEY, name_borough VARCHAR(50), zoneID BIGINT, FOREIGN KEY (zoneID) REFERENCES zone(zoneID));
CREATE TABLE ratecode (ratecodeID BIGINT PRIMARY KEY, name_rate_code VARCHAR(50));
CREATE TABLE payment_type (payment_typeID BIGINT PRIMARY KEY, name_payment_type VARCHAR(50));

--
-- Ajout des données dans les tables 
--

INSERT INTO vendor (vendorID, name_vendor) VALUES (1, 'Creative Mobile Technologies, LLC'), (2, 'VeriFone Inc'),(6, 'Fournisseur Mystere');
INSERT INTO ratecode(ratecodeID, name_rate_code) VALUES (1, 'Standard rate'), (2, 'JFK'),(3, 'Newark'), (4, 'Nassau or Westchester'), (5, 'Negotiated fare'), (6, 'Group ride'), (99, 'Code mystere');
INSERT INTO payment_type (payment_typeid, name_payment_type) VALUES (0,'Type NULL'), (1,'Credit card'), (2, 'Cash'), (3, 'No charge'), (4, 'Dispute'), (5, 'Unknown'), (6, 'Voided trip');

--Ajustement des données via le programme python "Integration_csv.py"
COPY service_zone FROM '/home/service_zone.csv' WITH CSV HEADER DELIMITER ','
COPY zone FROM '/home/zone.csv' WITH CSV HEADER DELIMITER ',';
COPY location FROM '/home/location.csv' WITH CSV HEADER DELIMITER ',';

--
-- Modification de la table principale pour ajouter les clés étrangères
--

ALTER TABLE nyc_raw ADD CONSTRAINT fk_vendor FOREIGN KEY (vendorid) REFERENCES vendor(vendorid);
ALTER TABLE nyc_raw ADD CONSTRAINT fk_ratecode FOREIGN KEY (ratecodeid) REFERENCES ratecode(ratecodeID);
ALTER TABLE nyc_raw ADD CONSTRAINT fk_payment_type FOREIGN KEY (payment_type) REFERENCES payment_type(payment_typeid);
ALTER TABLE nyc_raw ADD CONSTRAINT fk_pulocation FOREIGN KEY (pulocationid) REFERENCES location(locationID);
ALTER TABLE nyc_raw ADD CONSTRAINT fk_dolocation FOREIGN KEY (dolocationid) REFERENCES location(locationID);
