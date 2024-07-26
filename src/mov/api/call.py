def req(dt="20120101"):
    base_url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
    key = "2820df87eeb6a918654941ca8a918b2b"
    url = f"{base_url}?key={key}&targetDt={dt}"
    print(url)

