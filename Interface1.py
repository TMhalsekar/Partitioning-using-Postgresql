import psycopg2
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    createDB()
    con = openconnection
    cur = con.cursor()
    cur.execute("create table " + ratingstablename + "(userid integer, dummy1 char, movieid integer, dummy2 char, rating float, dummy3 char, timestamp bigint);")
    cur.copy_from(open(ratingsfilepath),ratingstablename,sep=':')
    cur.execute("alter table " + ratingstablename + " drop column dummy1, drop column dummy2, drop column dummy3, drop column timestamp;")
    cur.close()
    con.commit()

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions <= 0:
        print("Invalid number, please type an integer greater than zero")
        return
    con = openconnection
    cur = con.cursor()
    mid = 5 / numberofpartitions

    for i in range(0, numberofpartitions):
        lowerbound = i * mid
        upperbound = lowerbound + mid
        tname = RANGE_TABLE_PREFIX + str(i)
        cur.execute("create table " + tname + " (userid integer, movieid integer, rating float);")
        if i == 0:                                                                                          #seperate case for zero cause ranges will overlap after first partition
            cur.execute("insert into " + tname + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating >= " + str(lowerbound) + " and rating <= " + str(upperbound) + ";")
        else:
            cur.execute("insert into " + tname + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating > " + str(lowerbound) + " and rating <= " + str(upperbound) + ";")
    cur.close()
    con.commit()

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions <= 0:
        print("Invalid number, please type an integer greater than zero")
        return
    con=openconnection
    cur=con.cursor()

    npart=numberofpartitions
    for i in range(0,numberofpartitions):
        tname = RROBIN_TABLE_PREFIX + str(i)
        cur.execute("create table " + tname + " (userid integer, movieid integer, rating float);")
    for i in range(0,numberofpartitions):
        tname = RROBIN_TABLE_PREFIX + str(i)
        print(tname)
        #cur.execute("insert into"+tname+"(userid,movieid,rating) select userid,movieid,rating from (select row_number() over() as row from" +ratingstablename +")
        cur.execute("insert into " + tname + " (userid, movieid, rating) select userid, movieid, rating from (select userid, movieid, rating, row_number() over() as row from " + ratingstablename + ") as dummy where (dummy.row-1)%5 = " + str(i) + ";")
    cur.close()
    con.commit()

def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):

    con = openconnection
    cur = con.cursor()
    cur.execute("Insert INTO "+ratingstablename+ "(userid, movieid, rating) values(" + str(userid)+ "," + str(itemid)+","+str(rating)+")")
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + RROBIN_TABLE_PREFIX + "%';")
    p_count = cur.fetchone()[0]
    print (p_count)
    l1=[]
    for i in range(0,p_count):
        cur.execute("Select count(*) from "+RROBIN_TABLE_PREFIX+ str(i))
        a = cur.fetchone()[0]
        print(a)
        l1.append(a)
    print(l1)
    pos=None
    for i in range(1,len(l1)):
        if l1[i]< l1[i-1]:
            pos=i
            break
    if pos is None:
        pos=0
    cur.execute("INSERT INTO " + RROBIN_TABLE_PREFIX +str(pos) + " VALUES( " + str(userid)+ " , " + str(itemid) + " , " + str(rating) +")")
    cur.close()
    con.commit()


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):

    con = openconnection
    cur = con.cursor()
    cur.execute("Insert INTO " + ratingstablename + "(userid, movieid, rating) values(" + str(userid) + "," + str(itemid) + "," + str(rating) + ")")
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + RANGE_TABLE_PREFIX  + "%';")
    p_count = cur.fetchone()[0]
    num=5/p_count
    s1=0
    for x in range(0,p_count):
        tname = RANGE_TABLE_PREFIX+str(x)
        if ((s1 == 0 and rating >= s1 and rating <= s1+num)
            or (rating > s1 and rating <= s1+num)):
            cur.execute("Insert INTO " + tname + " values( " + str(userid)+ " , " + str(itemid) + " , " + str(rating) +")")
            break
        s1 += num
    cur.close()
    con.commit()
def createDB(dbname='dds_assignment1'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
