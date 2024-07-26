from mov.api.call import gen_url, req

def test_유알엘테스트():
    url = gen_url()
    assert "http" in url
    assert "kobis" in url

def test_req():
    code, data = req()
    assert code == 200

    code, data = req('20230101')
    assert code == 200

