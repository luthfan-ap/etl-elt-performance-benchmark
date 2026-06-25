<!-- ETL RUNNING STEPS -->
env JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home" SPARK_LOCAL_IP="127.0.0.1" python src/etl/etl_query9.py

<!-- ELT RUNNING STEPS -->
env JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home" SPARK_LOCAL_IP="127.0.0.1" python src/elt/elt_query9.py

cd dbt_transform

dbt run --select elt

<!-- HYBRID RUNNING STEPS -->
env JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home" SPARK_LOCAL_IP="127.0.0.1" python src/hybrid/hybrid_query9.py

cd dbt_transform

dbt run --select hybrid


<!-- JANGAN LUPA GANTI FILE_FORMAT NYA YA !! -->




<!-- TRUNCATE TABLE SEBELUM NGERUN -->
truncate table
    raw.elt_lineitem,
    raw.elt_nation,
    raw.elt_orders,
    raw.elt_part,
    raw.elt_partsupp,
    raw.elt_supplier
restart identity cascade;

truncate table
    raw.hybrid_lineitem,
    raw.hybrid_nation,
    raw.hybrid_orders,
    raw.hybrid_part,
    raw.hybrid_partsupp,
    raw.hybrid_supplier
restart identity cascade;
