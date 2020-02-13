PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE addresses(
                lat REAL NOT NULL,
                lon REAL NOT NULL,
                number TEXT,
                street TEXT NOT NULL,
                unit TEXT,
                city TEXT,
                district TEXT,
                region TEXT,
                postcode TEXT,
                PRIMARY KEY (lat, lon, number, street, city)
            );

-- 10 addresses with non-trivial duplicatas

INSERT INTO addresses VALUES(48.85504,2.22249,'65','Quai Marcel Dassault',NULL,'Saint-Cloud',NULL,NULL,'92210');
INSERT INTO addresses VALUES(48.85497,2.22255,'65','quai marcel dassault',NULL,'saint cloud',NULL,NULL,'92210');
INSERT INTO addresses VALUES(48.85504,2.22248,'65','quai marcel dassault',NULL,NULL,NULL,NULL,NULL);

INSERT INTO addresses VALUES(44.18536,0.66359,'1500','Avenue du Docteur Jean Noguès',NULL,'Boé',NULL,NULL,'47550');
INSERT INTO addresses VALUES(44.18532,0.66367,'1500','Avenue Dr. Jean Noguès',NULL,'Boé',NULL,NULL,'47550');
INSERT INTO addresses VALUES(44.18535,0.66366,'1500','av. du dr. jean noguès',NULL,NULL,NULL,NULL,NULL);

INSERT INTO addresses VALUES(48.99685,2.18891,'177','Boulevard Victor Bordier',NULL,'Montigny-lès-Cormeilles',NULL,NULL,'95370');
INSERT INTO addresses VALUES(48.99682,2.18897,'177','Bd. Victor Bordier',NULL,NULL,NULL,NULL,NULL);
INSERT INTO addresses VALUES(48.99686,2.18893,'177','boulevard victor bordier',NULL,NULL,NULL,NULL,NULL);

INSERT INTO addresses VALUES(45.69705,4.82488,'126','Boulevard de l''Europe',NULL,NULL,NULL,NULL,'69310');
INSERT INTO addresses VALUES(45.69707,4.82488,'126','Boulevard de l Europe',NULL,NULL,NULL,NULL,NULL);
INSERT INTO addresses VALUES(45.69700,4.82497,'126','bvd de l europe',NULL,NULL,NULL,NULL,'69310');

INSERT INTO addresses VALUES(45.70583,4.87766,'61','Avenue De La République',NULL,NULL,NULL,NULL,NULL);
INSERT INTO addresses VALUES(45.70579,4.87765,'61','av. république',NULL,NULL,NULL,NULL,NULL);

INSERT INTO addresses VALUES(44.08969,6.23707,'8','Avenue du 8 Mai 1945',NULL,'Digne-les-Bains',NULL,NULL,'04000');
INSERT INTO addresses VALUES(44.08976,6.23711,'8','Avenue du 8 Mai 1945',NULL,'Digne-les-Bains',NULL,NULL,'04000');
INSERT INTO addresses VALUES(44.08978,6.23712,'8','Avenue du 8 Mai 1945',NULL,'Digne-les-Bains',NULL,NULL,'04000');

INSERT INTO addresses VALUES(48.94847,2.34279,'53','Route de Saint-Leu',NULL,'Épinay-sur-Seine',NULL,NULL,'93800');
INSERT INTO addresses VALUES(48.94840,2.34275,'53','route de saint leu',NULL,'Épinay-sur-Seine',NULL,NULL,'93800');
INSERT INTO addresses VALUES(48.94840,2.34284,'53','rt. saint leu',NULL,'Épinay-sur-Seine',NULL,NULL,'93800');

INSERT INTO addresses VALUES(49.03465,3.39182,'26','Avenue de l''Europe',NULL,'Château-Thierry',NULL,NULL,'02400');
INSERT INTO addresses VALUES(49.03460,3.39190,'26','av. de l''europe',NULL,NULL,NULL,NULL,'02400');

INSERT INTO addresses VALUES(49.03460,3.3925,'26','Route de l''Europe',NULL,'Château-Thierry',NULL,NULL,'02400');
INSERT INTO addresses VALUES(49.03460,3.3937,'26','route de l europe',NULL,'Château-Thierry',NULL,NULL,'02400');

INSERT INTO addresses VALUES(49.03463,3.3935,'27','route de l europe',NULL,'Château-Thierry',NULL,NULL,'02400');
INSERT INTO addresses VALUES(49.03460,3.3935,'27','route de l''europe',NULL,'Château-Thierry',NULL,NULL,NULL);

COMMIT;
