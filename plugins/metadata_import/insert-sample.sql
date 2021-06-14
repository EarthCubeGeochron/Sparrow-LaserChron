/*
Short insert statement to assist in adding new samples and metadata to db
*/

INSERT INTO sample (name, material, location) VALUES(
    :name,
    :material,
    ST_GeomFromEWKT(:location)
);