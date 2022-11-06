import json5 as j5
from bs4 import BeautifulSoup as bs
import requests
import psycopg2 as ppg


# Scrape a web table to get data on La Liga clubs, format data and store in db
def find_team_data(cur):
    team_list = []

    # Team URL to scrape
    url_team = 'https://www.transfermarkt.us/la-liga/startseite/wettbewerb/ES1'
    header = {
      "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/50.0.2661.75 Safari/537.36",
      "X-Requested-With": "XMLHttpRequest"
    }

    page_team = requests.get(url_team, headers=header)

    # Scrape Team Data
    teams = bs(page_team.content, 'html.parser')
    lists = teams.find('table', attrs={'class': 'items'})
    list_body = lists.find('tbody')

    rows = list_body.find_all('tr')
    # Iterate over rows and columns
    for row in rows:
        cols = row.find_all('td')
        cols = [col.text.strip() for col in cols]
        team_list.append([col for col in cols if col])

    # Format for database entry
    for i in team_list:
        team_name = i[0]
        mil_euros = (i[5][1:][:-1])
        mv_euros = int(float(mil_euros)*1000000)
        fore = i[3]

        # Insert into database
        cur.execute("begin;"
                    "insert into stats.clubs (club, mv_euros, num_foreigners) values ('{}', {}, {});"
                    "commit;".format(team_name, mv_euros, fore))

# Scrape a web table to get data on La Liga players, format data, and store in db
def find_player_data(cur):
    # Players
    # Player URL to scrape
    url_player = 'https://www.capology.com/es/la-liga/salaries/'
    page_player = requests.get(url_player)

    # Scrape PLayer Data
    players = bs(page_player.text, 'lxml')
    lists = players.find_all('script')
    scri = str(lists[13])

    # A lot of formatting/parsing for this round (Java to JSON)
    step1 = scri.split('var data = ', 1)
    step2 = step1[1].split(';\n', 1)
    step3 = step2[0].replace('"', '')
    step4 = step3.replace('verified\':', 'verified\': ')
    step5 = step4.replace('length\':', 'length\': ')
    step6 = step5.replace(': ', ': "')
    step7 = step6.replace(',\n', '",\n')
    step8 = step7.replace('\': ', '": ')
    step9 = step8.replace('\n\'', '\n"')
    step10 = step9.replace('            \'', '"')

    # convert the pseudo-javascript to JSON format using json5
    py_object = j5.loads(step10)

    # A little bit more parsing and prep for database entry
    for i in py_object:
        name = i['name'].split('\'lazy\'>')[1].split('</a')[0]
        status = i['status']
        position = i['position']
        position_detail = i['position_detail']
        age = i['age'].split('(')[1].split(')')[0]
        country = i['country']
        club = i['club'].split('\'>')[1].split('<')[0]
        mv = i['annual_gross_eur'].split('(')[1].split(',')[0]

        # Correct club name inconsistency across sources
        if club == 'Almeria':
            club = 'UD Almería'
        if club == 'Atletico Madrid':
            club = 'Atlético de Madrid'
        if club == 'Barcelona':
            club = 'FC Barcelona'
        if club == 'Cadiz':
            club = 'Cádiz CF'
        if club == 'Celta Vigo':
            club = 'Celta de Vigo'
        if club == 'Elche':
            club = 'Elche CF'
        if club == 'Espanyol':
            club = 'RCD Espanyol Barcelona'
        if club == 'Girona':
            club = 'Girona FC'
        if club == 'Osasuna':
            club = 'CA Osasuna'
        if club == 'Real Betis':
            club = 'Real Betis Balompié'
        if club == 'Sevilla':
            club = 'Sevilla FC'
        if club == 'Valencia':
            club = 'Valencia CF'
        if club == 'Valladolid':
            club = 'Real Valladolid CF'
        if club == 'Villarreal':
            club = 'Villarreal CF'
        if club == 'Athletic Club':
            club = 'Athletic Bilbao'
        if club == 'Getafe':
            club = 'Getafe CF'
        if club == 'Mallorca':
            club = 'RCD Mallorca'

        # Insert data into database
        cur.execute("begin;"
                    "insert into stats.players "
                    "(player_name, status, primary_position, secondary_position, age, country, club, mv_euros) values "
                    "('{}', '{}', '{}', '{}', {}, '{}', '{}', {}) on conflict do nothing;"
                    "commit;".format(name, status, position, position_detail, age, country, club, mv))


# Query to obtain the total market value of each team in each position
def get_db_data(cur):
    distinct_clubs = []
    distinct_positions = []
    long_position = ''

    # Get list of clubs
    cur.execute("select club from stats.clubs;")
    list_clubs = cur.fetchall()
    for i in list_clubs:
        distinct_clubs.append(i[0])

    # Get list of distinct positions
    cur.execute("select distinct primary_position from stats.players;")
    list_positions = cur.fetchall()
    for i in list_positions:
        distinct_positions.append(i[0])

    # Step through each list to generate a sum of position values at each club
    for i in range(len(distinct_clubs)):
        for j in range(len(distinct_positions)):
            cur.execute("select sum(mv_euros) from stats.players where club = '{}' and primary_position = '{}';"
                        .format(distinct_clubs[i], distinct_positions[j]))
            final = cur.fetchall()

            # Use full name of position
            if distinct_positions[j] == 'K':
                long_position = 'keeper'
            if distinct_positions[j] == 'M':
                long_position = 'midfielder'
            if distinct_positions[j] == 'F':
                long_position = 'forward'
            if distinct_positions[j] == 'D':
                long_position = 'defender'

            print('The total market value of the {} position at {} position: {} Euros'
                  .format(long_position, distinct_clubs[i], str(final[0][0])))


if __name__ == "__main__":
    # Connection parameters
    database = 'liga'
    user = 'postgres'
    password = ' '
    host = 'localhost'

    # Connect to DB
    conn = ppg.connect("dbname=\'{}\' user=\'{}\' password=\'{}\' host=\'{}\'"
                       .format(database, user, password, host))
    cur = conn.cursor()

    # Execute function to compile club data
    find_team_data(cur)

    # Execute function to compile player data
    find_player_data(cur)

    # Execute function to display market value info per club per position
    get_db_data(cur)

    cur.close()
    conn.close()
