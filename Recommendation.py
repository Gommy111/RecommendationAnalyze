import psycopg2
from psycopg2.extras import RealDictCursor
import datetime

#   Setup
c = psycopg2.connect("dbname=postgres user=postgres password=postgres")
cur = c.cursor(cursor_factory=RealDictCursor)


#   Adds a support column to products and counts how many times the product has been viewed
#   This takes a really long time
def add_support():
    cur.execute("""DO $$ 
        BEGIN BEGIN 
        ALTER TABLE products ADD COLUMN support INTEGER; 
        EXCEPTION WHEN duplicate_column THEN RAISE NOTICE 'column support already exists in products.'; 
        END; END; $$""")
    print("support column created in products")
    cur.execute("SELECT COUNT(profid) FROM profiles_previously_viewed")
    total = cur.fetchall()[0]['count']
    print("total views", total)
    i = 0
    cur.execute("SELECT id FROM products")
    for row_id in cur.fetchall():
        prod_id = row_id['id']
        cur.execute("SELECT COUNT(prodid) FROM profiles_previously_viewed WHERE prodid = %s", (prod_id,))
        count = cur.fetchall()[0]['count']
        cur.execute("UPDATE products SET support = %s WHERE id = %s", (count/total, prod_id))
        print(i, 'of', total)
        i += 1
    c.commit()


def preprocces():
    add_support()
    c.commit()


#   Looks at the differences of properties between rows,
#   the more similar the properties of the rows are, the lower distance
def distance_rows(table, row_id1, row_id2, distance_rules) -> float:
    distance_rules: dict
    distance = 0.0
    cur.execute("SELECT * FROM {} WHERE id = %s OR id = %s".format(table), (row_id1, row_id2))
    prod1, prod2 = cur.fetchall()

    for i in range(len(distance_rules)):
        field = tuple(distance_rules.keys())[i]
        func = tuple(distance_rules.values())[i]
        distance += func(prod1[field], prod2[field])

    return distance


def rule1(category, subcategory, susubcategory, targetaudience):
    cur.execute("""SELECT id FROM products WHERE 
    category = %s AND subcategory = %s AND subsubcategory = %s AND targetaudience = %s""",
                (category, subcategory, susubcategory, targetaudience))
    ouput = []
    i = 0
    for row in cur.fetchall():
        ouput.append(row['id'])
        i += 1
        if i >= 10:
            break
    return ouput


def rule2(segment):
    cur.execute("SELECT id FROM profiles WHERE segment = %s", (segment,))
    ouput = []
    i = 0
    for row in cur.fetchall():
        ouput.append(row['id'])
        i += 1
        if i >= 10:
            break
    return ouput


def datetime_difference(string1, string2):
    string_format = '%Y-%m-%d %H:%M:%S.%f'
    datetime1 = datetime.datetime.strptime(string1, string_format)
    datetime2 = datetime.datetime.strptime(string2, string_format)
    return (datetime1 - datetime2).total_seconds()


products_distance_rules = {
        'brand': lambda a, b: (a != b) * 1,
        'type': lambda a, b: (a != b) * 1,
        'category': lambda a, b: (a != b) * 1,
        'subcategory': lambda a, b: (a != b) * 1,
        'subsubcategory': lambda a, b: (a != b) * 1,
        'targetaudience': lambda a, b: (a != b) * 5,
        'sellingprice': lambda a, b: abs(a - b) / 500,
        'deal': lambda a, b: (a != b) * 1
    }
profiles_distance_rules = {
        'segment': lambda a, b: (a != b) * 5,
        'latestactivity': lambda a, b: datetime_difference(str(a), str(b)) / (60*60*24*365)
    }

#   Just tests so far
if __name__ == '__main__':
    # print(distance_rows('products', '7225', '29438', products_distance_rules))
    # print(distance_rows('profiles', '5a393d68ed295900010384ca', '5a393eceed295900010386a8', profiles_distance_rules))
    # print(datetime_difference("2019-01-13 14:08:33.995", "2018-04-19 10:13:28.391"))
    # preprocces()
    print(rule1('Gezond & verzorging', 'Lichaamsverzorging', 'Deodorant', 'Vrouwen'))
    print(rule2("BOUNCER"))
    c.commit()
    cur.close()
    c.close()
