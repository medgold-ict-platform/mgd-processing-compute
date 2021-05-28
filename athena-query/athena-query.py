import boto3
import time


client = boto3.client('athena')
datasets = ['era5', 'ecmwf', 'e-obs']
code = ['7U', '8O', '14W', 'GDD', 'GST']

if __name__ == '__main__':
    try:
        for dataset in datasets:
            for year in range(1989, 2019):
                i = 0
                l = []
                # cumulative number (count) of days with Tmax above 28C in April May and June 
                index7U = '''select a.longitude, a.latitude, coalesce(b.nofday, 0) as olive_7
                    from {0}.t2m_maxmax as a LEFT OUTER JOIN 
                    (select latitude, longitude, count(*) as nofday
                    from (select distinct *
                    from {0}.t2m_maxmax
                    where cast(time as date) >= cast('{1}-04-01' as date) and cast(time as date) <= cast('{1}-06-30' as date) and (t2m - 273.15) > 28.0)
                    group by latitude, longitude)
                    as b on a.latitude = b.latitude and a.longitude = b.longitude
                    group by a.latitude, a.longitude, b.nofday'''.format(dataset,year)
                # cumulative number (count) of days with totalprecipitation below probably 2mm
                index8O = '''select a.longitude, a.latitude, coalesce(b.nofday, 0) as olive_8
                    from {0}.tp as a LEFT OUTER JOIN 
                    (select latitude, longitude, count(*) as nofday
                    from (select distinct *
                    from {0}.tp
                    where cast(time as date) >= cast('{1}-01-01' as date) and cast(time as date) <= cast('{1}-12-31' as date) and tp < 0.002)
                    group by latitude, longitude)
                    as b on a.latitude = b.latitude and a.longitude = b.longitude
                    group by a.latitude, a.longitude, b.nofday'''.format(dataset, year)
                # number of days with minimum daily temperature below 2 deg. C between April and May
                index14W= '''select a.longitude, a.latitude, coalesce(b.nofday, 0) as wheat_14
                    from {0}.t2m_minmin as a LEFT OUTER JOIN 
                    (select latitude, longitude, count(*) as nofday
                    from (select distinct *
                    from {0}.t2m_minmin
                    where cast(time as date) >= cast('{1}-04-01' as date) and cast(time as date) <= cast('{1}-05-31' as date) and (t2m - 273.15) < 2.0)
                    group by latitude, longitude)
                    as b on a.latitude = b.latitude and a.longitude = b.longitude
                    group by a.latitude, a.longitude, b.nofday'''.format(dataset, year)
                # the summation of daily differences between daily meantemperatures and 10 C between April 1st andOctober 31st (Northern Hemisphere).
                indexGDD='''select a.longitude, a.latitude, coalesce(b.gst, 0) as GDD
                    from {0}.t2m_avg as a LEFT OUTER JOIN 
                        (select latitude, longitude, sum(t2m) as gst
                        from 
                            (select distinct latitude, longitude, (t2m-283.15) as t2m, time
                            from {0}.t2m_avg
                            where cast(time as date) >= cast('{1}-04-01' as date) 
                                                    and cast(time as date) <= cast('{1}-10-31' as date) 
                                                    and latitude > 0 and t2m-273.15 > 10)
                        where t2m > 10
                        group by latitude, longitude)
                    as b on a.latitude = b.latitude and a.longitude = b.longitude and a.latitude > 0
                    group by a.latitude, a.longitude, b.gst'''.format(dataset, year)
                # defined as theaverage of daily mean temperatures between April 1st and October 31st(Northern Hemisphere)
                indexGST = '''select a.longitude, a.latitude, coalesce(b.gst, 0) as GST
                    from {0}.t2m_avg as a LEFT OUTER JOIN 
                        (select latitude, longitude, avg(t2m-273.15) as gst
                        from 
                            (select distinct *
                            from {0}.t2m_avg
                            where cast(time as date) >= cast('{1}-04-01' as date) 
                                                    and cast(time as date) <= cast('{1}-10-31' as date) 
                                                    and latitude > 0)
                        group by latitude, longitude)
                    as b on a.latitude = b.latitude and a.longitude = b.longitude and a.latitude > 0
                    group by a.latitude, a.longitude, b.gst'''.format(dataset, year)
                l = [indexGST, indexGDD, index14W, index8O, index7U]
                for index in l:
                    response = client.start_query_execution(
                        QueryString=index,
                        QueryExecutionContext={
                            'Database': '{0}'.format(dataset)
                        },
                        ResultConfiguration={
                            'OutputLocation': "s3://dev.index-calculation.{0}.athena-query/{1}/{0}/".format(dataset, code[i])+str(year)
                        }
                    )
                    i = i + 1
    except Exception as e:
       raise


