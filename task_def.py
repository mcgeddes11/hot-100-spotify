import luigi, pandas, os, bs4, requests, json, time, urllib2



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


class GetTrackUrlsByYear(luigi.Task):
    config = luigi.Parameter()
    year = luigi.Parameter()

    def requires(self):
        return GetSongsByYear(self.config, self.year)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.config["data_repository"], "SongUrlsCreated_" + str(self.year) + ".csv"))

    def run(self):

        songlist = pandas.read_csv(self.input().path)
        qryBase = self.config["spotify_search_url"]
        trackUris = []
        # Loop over songs
        for ix, track in songlist.iterrows():
            complete = False
            #TODO: Remove punctuation and other bad chars
            qry = qryBase.replace("[query]", urllib2.quote(track["artist"] + " " + track["title"]))
            # use search method to return result
            while not complete:
                r = requests.get(qry)
                if r.status_code == 200:
                    data = json.loads(r.text)
                    complete = True
                elif r.status_code == 429:
                    timeTillRetry = r.headers["Retry-After"]
                    time.sleep(timeTillRetry + 5)
                else:
                    print "error: " + str(r.status_code)
            # Only check first element for now
            if len(data["tracks"]["items"]) == 0:
                trackUris.append("not_found")
            else:
                first = data["tracks"]["items"][0]
                artistCheck = track["artist"] in [x["name"] for x in first["artists"]]
                nameCheck = first["name"] == track["title"]
                if artistCheck or nameCheck:
                    trackUris.append(first["uri"])
                else:
                    trackUris.append("not_found_first")


            # add result to data frame
            print track
        songlist["URI"] = trackUris
        open(self.output().path, 'w').close()
        songlist.to_csv(self.input().path, index=False)

