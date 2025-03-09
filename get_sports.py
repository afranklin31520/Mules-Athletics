import requests , collections , bs4
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import numpy as np
class mule_scraper:
    def __init__(self,link:str):
        res = requests.get(link,headers={"User-Agent": "Mozilla/5.0"})
        self.status_code = res.status_code
        self.parser = BeautifulSoup(res.text,'html.parser')
        self.BASE_LINK = "https://muhlenbergsports.com"
        self.HOME_PAGE = "https://muhlenbergsports.com/index.aspx"
        self.parser.prettify()
    def change_link(self,link:str):
        self.__init__(link)
    def get_initial_df(self):
        cols = ["name","facebook","twitter","instagram","schedule","roster","news"]
        df = collections.defaultdict(list)
        tags = self.parser.find("div",{"class":"hide"}).find_all("a")
        for idx in range(0,len(tags),7):
            if idx >= 147:
                break
            temp_row = [tags[i].text.strip() if i % 7 == 0 else tags[i]["href"].replace(r"//",r"/") for i in range(idx,idx+7)]
            for col , val in zip(cols,temp_row):df[col].append(val)
        pd.DataFrame(df).to_csv("teams.csv")
    def win_loss_ratio(self):
        df = pd.read_csv("teams.csv")
        df = df.iloc[:,1:]
        for col in ['Overall', 'PCT', 'Conf', 'Streak', 'Home', 'Away', 'Neutral']:
            df[col] = np.nan
        length , width = df.shape
        for row in range(length):
            full_url = self.BASE_LINK + df['schedule'].iloc[row]
            self.change_link(full_url)
            table = self.parser.find("div",{"aria-label": "Schedule Record"})
            if table:
                items = table.find_all("li")
                # Create dictionary
                data = {}
                for item in items:
                    key, value = item.find_all("span")
                    data[key.text.strip()] = value.text.strip()
                df.loc[row,list(data.keys())] = list(data.values())
            else:
                print(f"{df['name'].iloc[row]} does not keep a record!")
        df.to_csv("teams_wl.csv")
    def get_player_roster(self):
        df = pd.read_csv("teams.csv")
        df = df.iloc[:,1:]
        length , width = df.shape
        common_headers = ['Image', 'Name', 'Yr.', 'Hometown', 'C', 'team', 'Wt', 'High School', 'Ht','Team','short_name','year']
        players = pd.DataFrame(
                columns = common_headers
            )
        popularity_df = pd.DataFrame(
                            columns= ["team","year","news_count"])
        for row in range(length):
            full_url = self.BASE_LINK + df['roster'].iloc[row]
            team = df.iloc[row]['name']
            self.change_link(full_url)
            years = self.parser.find("select",{"id":"ddl_past_rosters"})
            if years:
                options = years.find_all("option")
                print("Loading roster for",df.iloc[row]['short_name'])
                for option in options:
                    new_link = "https://muhlenbergsports.com" + option['value']
                    print("Processing",new_link)
                    year_val = (option['value'].split("/")[-1].split("-")[0])
                    temp_df = self.get_player_roster_per_year(new_link,df.iloc[row]['short_name'],year_val)
                    temp_df['year']
                    if temp_df is not None and not temp_df.empty:
                        players = pd.concat([players,temp_df])
                        print("status for",new_link,"success!")
                        popularity_df = pd.concat([popularity_df,self.get_news(df.iloc[row]['short_name'],year_val)])
                print("finished!")
            else:
                print("status for",new_link,"failed!")
        print("common headers are",common_headers)
        players[common_headers].to_csv("players.csv")
        popularity_df.to_csv("popularity.csv")
    def get_player_roster_per_year(self,link,short_name,year):
        self.change_link(link)
        table = self.parser.find("table",{"class":"sidearm-table sidearm-table-grid-template-1 sidearm-table-grid-template-1-breakdown-large"})
        if table:
            temp_df = pd.read_html(StringIO(table.decode()))[0]
            temp_df.rename(columns={"Academic Year":"Yr.","Full Name":"Name","Wt.":"Wt","Ht.":"Ht"},inplace=True)
            for header in ["Wt","Ht"]:
                if header not in temp_df.columns:
                    temp_df[header] = 0
            if "C" not in temp_df.columns:
                temp_df["C"] = np.nan
            temp_df['short_name'] = short_name
            temp_df['year'] = year
            return temp_df
    def get_news(self,short_name,year):
        hidden_api_link = "https://muhlenbergsports.com/services/archives.ashx/stories?index=1&page_size=1&sport={}&season={}&search=".format(short_name,year)
        print(hidden_api_link)
        sub_res = requests.get(hidden_api_link,headers={"User-Agent": "Mozilla/5.0"}).json()
        news_postings = sub_res["data"]
        popularity_df = pd.DataFrame(
                            columns= ["team","year","news_count"]
                        )
        if len(news_postings) < 1:
            news_postings = 0
        else:
            news_postings = news_postings[0]["row_count"]
        popularity_df['team'] = short_name
        popularity_df['year'] = year
        popularity_df['news_count'] = news_postings
        popularity_df.add
        return pd.DataFrame(
            [{
        'team' : short_name,
        'year' : year,
        'news_count' : news_postings
            }]
        )
    def get_schedule(self):
        df = pd.read_csv("teams.csv")
        length , width = df.shape
        for row in range(length):
            full_url = "https://muhlenbergsports.com" + df.iloc[row]['schedule']
            self.change_link(full_url)
            schedule = self.parser.find_all("a",{"target":"_blank"},role=False,class_=False)[1:-1]
            for game in schedule:
                print(game)
    def load_historical_stats(self,link:str,sport_name:str):
        df = pd.read_csv("teams.csv")
        l ,w = df.shape
        team_table_captions = [
            "Game By Game - Team Statistics",
            "Game By Game Results",
            "Game-By-Game Game Results Statistics",
            "Match-By-Match Team Statistics",
            "Game-By-Game Team Statistics",
        ]
        self.change_link(link)
        team_stats = self.parser.find("section",{"id":"game"})
        if team_stats:
            tables = team_stats.find_all("table")
            print(f"{sport_name} has {len(tables)} tables")
            for table in tables:
                if table.find("caption").text in team_table_captions:
                    team_table = pd.read_html(StringIO(table.decode()))[0]
                    return team_table
        else:
            print(f"{sport_name} does not have stats page")
    def get_all_stat_pages(self):
        df = pd.read_csv("teams.csv")
        l ,w = df.shape
        for row in range(l):
            sport_name = df.iloc[row]['roster'].split("/")[-2]
            full_url = "https://muhlenbergsports.com/sports/" + sport_name + "/stats#game"
            self.change_link(full_url)
            team_stats = self.parser.find("section",{"id":"game"})
            if team_stats:
                seasons = self.parser.find("select",{'aria-label':'Select a Season'})
                for main_option in seasons.find("option"):
                    if type(main_option) == bs4.element.Tag:
                        stat_links = [
                            "https://muhlenbergsports.com" + option['value'] for option in main_option.find_all("option")
                        ]
                        all_dfs = [
                            self.load_historical_stats(sub_link,sport_name) for sub_link in stat_links
                        ]
                        historical_team_df = pd.concat(all_dfs,ignore_index=True)
                        historical_team_df['team_name'] = df.iloc[row]['short_name']
                        historical_team_df.to_csv(f"team_stats/{df.iloc[row]['short_name']}.csv")
            else:
                self.get_stats_by_schudule(link="https://muhlenbergsports.com"+df.iloc[row]["schedule"],short_name=df.iloc[row]['short_name'])
    def get_stats_by_schudule(self,link:str,short_name:str):
        self.change_link(link)
        temp_dict = {
            "date":[],
            "W/L":[],
            "team_name":[]
        }
        seasons = self.parser.find("select",{"id":"sidearm-schedule-select-season"})
        for option in seasons.find_all("option"):
            self.change_link("https://muhlenbergsports.com"+option["value"])
            record = self.parser.find("li",{"class":"large-flex-item-1 flex flex-column flex-justify-center flex-align-center x-small-3 columns"})
            if record:
                year = int(option["value"].split("/")[-1].split("-")[0])
                overall_record = record.find_all("span",{"class":"flex-item-1"})[-1].text
                win , loss = overall_record.split("-")
                win , loss = int(win) , int(loss)
                if loss != 0:
                    for i in range(win):
                        temp_dict["date"].append(f"01/01/{year}")
                        temp_dict["W/L"].append("W")
                        temp_dict["team_name"].append(short_name)
                    for i in range(loss):
                        temp_dict["date"].append(f"01/01/{year}")
                        temp_dict["W/L"].append("L")
                        temp_dict["team_name"].append(short_name)
            else:
                print("https://muhlenbergsports.com"+option["value"],"does not have a record!")
        if len(temp_dict['date']) > 0:
            df = pd.DataFrame(temp_dict)
            df['Opponent'] = np.nan
            df.to_csv(f"team_stats/{short_name}.csv")
    def get_player_count(self):
        df = pd.read_csv("teams.csv")
        l , w = df.shape
        df['player_count'] = 0
        for row in range(l):
            full_url = "https://muhlenbergsports.com" + df.loc[row,"roster"]
            self.change_link(full_url)
            roster = pd.read_html(StringIO(self.parser.find_all("table")[-2].decode()))[0]
            df.loc[row,"player_count"] = roster.shape[0]
        df.to_csv("teams.csv",index=False)
    
x = mule_scraper("https://muhlenbergsports.com/index.aspx")
x.get_all_stat_pages()
