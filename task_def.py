import luigi, pandas, sqlalchemy, logging, os, numpy, bs4, requests
from datetime import date, datetime, timedelta


class GetSongsByYear(luigi.Task):
    config = luigi.Parameter()
    year = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.config["data_repository"], "Songlist_" + str(self.year) + ".csv"))

    def run(self):
        url = self.config["jjj_url"].replace("[year]",str(self.year))
        html = requests.get(url).text
        soup = bs4.BeautifulSoup(html)
        songInfos = soup.find_all('ol')
        songInfos = songInfos[0].find_all("li")
        allSongs = {"position": [], "artist": [], "title": []}
        count = 1
        for song in songInfos:
            songTxt = song.getText()
            songTxt = songTxt.encode("ascii","ignore")
            songTxt = songTxt.split(" - ")
            allSongs["artist"].append(songTxt[0])
            allSongs["title"].append(songTxt[1])
            allSongs["position"].append(count)
            count += 1
        pandas.DataFrame(allSongs).to_csv(self.output().path, index=False)