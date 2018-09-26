# coding: utf-8
import datetime
from pyspark.sql import SparkSession, SQLContext


spark = SparkSession \
    .builder \
    .appName("PURCHASE_PROFILE_MOBILEVEN") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)

# Reading parquet file
df = sqlContext.read \
    .parquet("gs://prd-lake-raw-atena/erp_order/")

# Creates a temporary view using the DataFrame
df.createOrReplaceTempView("order")


# Geting date for the process
startdate = datetime.datetime.now().date() - datetime.timedelta(days=10)
startdate = startdate.strftime("%Y-%m-%d %H:%M:%S")


# Last customers for processing
last_custs = spark.sql(
  """
  select codcli
    from order
   where dtpedido >= '%s'
     and formapagtoprinc <> 'CREDDEVOL'
     and codfil <> 660
     and codcli <> 999999
   group by codcli
  """ % (startdate))

# Creates a temporary view using the DataFrame
last_custs.createOrReplaceTempView("last_customers")


# Getting all orders of Last customers
orders_full = spark.sql(
  """
  select o.codcli
        ,'amount' as Periodicity
        ,round(sum(if(o.codfil <> 200, o.vltotal, 0)), 2) vlr_physical
        ,round(sum(if(o.codfil = 200, o.vltotal, 0)), 2) vlr_digital
        ,sum(if(o.codfil <> 200, 1, 0)) qtd_physical
        ,sum(if(o.codfil = 200, 1, 0)) qtd_digital
        ,max(if(o.codfil <> 200, o.numpedven, 0)) numped_physical
        ,max(if(o.codfil = 200, o.numpedven, 0)) numped_digital
    from (select g.numpedprinc
                ,g.codcli
                ,g.codfil
                ,sum(g.vltotal) as vltotal
                ,max(g.numpedven) as numpedven
            from order g
                 INNER JOIN
                 last_customers c
                 ON (g.codcli = c.codcli)
           where g.codfil <> 660
             and g.formapagtoprinc <> 'CREDDEVOL'
           group by g.numpedprinc
                   ,g.codcli
                   ,g.codfil) o
   group by o.codcli
  """)

# Creates a temporary view using the DataFrame
orders_full.createOrReplaceTempView("orders_full")


# Geting date of 36 months ago
dt36months = datetime.datetime.now().date() - datetime \
    .timedelta(days=36*365/12)
dt36months = dt36months.strftime("%Y-%m-%d %H:%M:%S")

# Getting orders in 36 months of Last customers
orders_36_months = spark.sql(
  """
  select o.codcli
        ,'thirty-six-months' as Periodicity
        ,round(sum(if(o.codfil <> 200, o.vltotal, 0)), 2) vlr_physical
        ,round(sum(if(o.codfil = 200, o.vltotal, 0)), 2) vlr_digital
        ,sum(if(o.codfil <> 200, 1, 0)) qtd_physical
        ,sum(if(o.codfil = 200, 1, 0)) qtd_digital
    from (select g.numpedprinc
                ,g.codcli
                ,g.codfil
                ,sum(g.vltotal) as vltotal
            from order g
                 INNER JOIN
                 last_customers c
                 ON (g.codcli = c.codcli)
           where g.codfil <> 660
             and g.formapagtoprinc <> 'CREDDEVOL'
             and g.dtpedido >= '%s'
           group by g.numpedprinc
                   ,g.codcli
                   ,g.codfil) o
   group by o.codcli
  """ % (dt36months))

# Creates a temporary view using the DataFrame
orders_36_months.createOrReplaceTempView("orders_36_months")


# Geting date of 12 months ago
dt12months = datetime.datetime.now().date() - datetime \
    .timedelta(days=12*365/12)
dt12months = dt12months.strftime("%Y-%m-%d %H:%M:%S")

# Getting orders in 12 months of Last customers
orders_12_months = spark.sql(
  """
  select o.codcli
        ,'twelve-months' as Periodicity
        ,round(sum(if(o.codfil <> 200, o.vltotal, 0)), 2) vlr_physical
        ,round(sum(if(o.codfil = 200, o.vltotal, 0)), 2) vlr_digital
        ,sum(if(o.codfil <> 200, 1, 0)) qtd_physical
        ,sum(if(o.codfil = 200, 1, 0)) qtd_digital
    from (select g.numpedprinc
                ,g.codcli
                ,g.codfil
                ,sum(g.vltotal) as vltotal
            from order g
                 INNER JOIN
                 last_customers c
                 ON (g.codcli = c.codcli)
           where g.codfil <> 660
             and g.formapagtoprinc <> 'CREDDEVOL'
             and g.dtpedido >= '%s'
           group by g.numpedprinc
                   ,g.codcli
                   ,g.codfil) o
   group by o.codcli
  """ % (dt12months))

# Creates a temporary view using the DataFrame
orders_12_months.createOrReplaceTempView("orders_12_months")


# Geting date of 1 months ago
dt1month = datetime.datetime.now().date() - datetime.timedelta(days=1*365/12)
dt1month = dt1month.strftime("%Y-%m-%d %H:%M:%S")

# Getting orders in month of Last customers
orders_in_month = spark.sql(
  """
  select o.codcli
        ,'month' as Periodicity
        ,round(sum(if(o.codfil <> 200, o.vltotal, 0)), 2) vlr_physical
        ,round(sum(if(o.codfil = 200, o.vltotal, 0)), 2) vlr_digital
        ,sum(if(o.codfil <> 200, 1, 0)) qtd_physical
        ,sum(if(o.codfil = 200, 1, 0)) qtd_digital
    from (select g.numpedprinc
                ,g.codcli
                ,g.codfil
                ,sum(g.vltotal) as vltotal
            from order g
                 INNER JOIN
                 last_customers c
                 ON (g.codcli = c.codcli)
           where g.codfil <> 660
             and g.formapagtoprinc <> 'CREDDEVOL'
             and g.dtpedido >= '%s'
           group by g.numpedprinc
                   ,g.codcli
                   ,g.codfil) o
   group by o.codcli
  """ % (dt1month))

# Creates a temporary view using the DataFrame
orders_in_month.createOrReplaceTempView("orders_in_month")


# Last physical orders
last_order_physical = spark.sql(
  """
  select o.codcli
        ,'physical' as channel
        ,o.dtpedido as dt_last_purchase
        ,o.formapagtoprinc as last_means_payment
        ,o.qtdparcela as last_parcel_number
        ,'' as best_means_payment
    from order o,
        (select codcli
               ,numped_physical
           from orders_full
          where qtd_physical <> 0) u
   where o.numpedven = u.numped_physical
  """)

# Creates a temporary view using the DataFrame
last_order_physical.createOrReplaceTempView("last_order_physical")


# Last Digital orders
last_order_digital = spark.sql(
  """
  select o.codcli
        ,'digital' as channel
        ,o.dtpedido as dt_last_purchase
        ,o.formapagtoprinc as last_means_payment
        ,o.qtdparcela as last_parcel_number
        ,'' as best_means_payment
    from order o,
        (select codcli
               ,numped_digital
           from orders_full
          where qtd_digital <> 0) u
   where o.numpedven = u.numped_digital
  """)

# Creates a temporary view using the DataFrame
last_order_digital.createOrReplaceTempView("last_order_digital")


# Consolidation Customers for channels
customer_channels = spark.sql(
  """
  select t.*
    from
   (select l.codcli
          ,l.channel
          ,l.dt_last_purchase
          ,l.last_means_payment
          ,l.best_means_payment
          ,l.last_parcel_number
          ,o.vlr_physical as vlr_purchase
          ,o.qtd_physical as qtd_purchase
     from last_order_physical l
          INNER JOIN
          orders_full o
          ON (l.codcli = o.codcli)
    where o.qtd_physical <> 0
   UNION
    select l.codcli
          ,l.channel
          ,l.dt_last_purchase
          ,l.last_means_payment
          ,l.best_means_payment
          ,l.last_parcel_number
          ,o.vlr_digital as vlr_purchase
          ,o.qtd_digital as qtd_purchase
     from last_order_digital l
          INNER JOIN
          orders_full o
          ON (l.codcli = o.codcli)
    where o.qtd_digital <> 0) t
  """)

# Writing the consolidation Customers for channels in files
customer_channels.write.csv(
    "gs://prd-lake-trusted-atena/export/mobilevendas/customer_channels/",
    sep="|", mode="overwrite")


# Consolidation Customers for channels and Periodicity
cust_chan_period = spark.sql(
  """
  select t.codcli, t.channel, t.periodicity
        ,t.qtd_purchase, round(t.vlr_purchase,2) as vlr_purchase
    from (
      select codcli, 'digital' as channel, periodicity
            ,qtd_digital as qtd_purchase, vlr_digital as vlr_purchase
        from orders_36_months where qtd_digital <> 0
      UNION
      select codcli, 'physical' as channel, periodicity
            ,qtd_physical as qtd_purchase, vlr_physical as vlr_purchase
        from orders_36_months where qtd_physical <> 0
       UNION
      select codcli, 'digital' as channel, periodicity
            ,qtd_digital as qtd_purchase, vlr_digital as vlr_purchase
        from orders_12_months where qtd_digital <> 0
      UNION
      select codcli, 'physical' as channel, periodicity
            ,qtd_physical as qtd_purchase, vlr_physical as vlr_purchase
        from orders_12_months where qtd_physical <> 0
      UNION
      select codcli, 'digital' as channel, periodicity
            ,qtd_digital as qtd_purchase, vlr_digital as vlr_purchase
        from orders_in_month where qtd_digital <> 0
      UNION
      select codcli, 'physical' as channel, periodicity
            ,qtd_physical as qtd_purchase, vlr_physical as vlr_purchase
        from orders_in_month where qtd_physical <> 0) t
  """)

# Writing the consolidation Customers for channels and Periodicity in files
cust_chan_period.write.csv(
    "gs://prd-lake-trusted-atena/export/mobilevendas/cust_chan_period/",
    sep="|", mode="overwrite")
