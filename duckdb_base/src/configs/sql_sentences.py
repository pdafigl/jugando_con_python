create_total_nacional = """
    CREATE TABLE IF NOT EXISTS nacional (
        Periodo TIMESTAMP_NS,
        Porcentaje FLOAT8 NOT NULL,
        PRIMARY KEY(Periodo, Porcentaje)
    )
"""

create_total_local = """
    CREATE TABLE IF NOT EXISTS local (
        Periodo TIMESTAMP_NS,
        Localidad VARCHAR NOT NULL,
        Porcentaje FLOAT8 NOT NULL,
        PRIMARY KEY(Periodo, Localidad, Porcentaje)
    )
"""

insert_data_in_nacional = """
    INSERT INTO nacional 
    SELECT periodo, porcentaje FROM raw_data
    WHERE areas_movilidad LIKE 'Total Nacional'
    """
insert_data_in_local = """
    INSERT INTO local 
    SELECT periodo, areas_movilidad, porcentaje FROM raw_data
    WHERE areas_movilidad NOT LIKE 'Total Nacional'
    """