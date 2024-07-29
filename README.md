# mov

### install
```bash
# main
$ pip install git+https://github.com/dMario24/mov.git

# branch
$ pip install git+https://github.com/dMario24/mov.git@<BRANCH_NAME>
```

### start dev
```bash
$ git clone <URL>
$ cd <DIR>
$ source .venv/bin/activate
$ pdm install
$ pytest

# option
$ pdm venv create
```

### setting env
```bash
cat ~/.zshrc | tail -n 3

# MY_ENV
export MOVIE_API_KEY="<KEY>"
```

### 트러블슈팅
- [ ] 영화진흥위원회 가입 및 키생성
```
{'faultInfo': {'message': '유효하지않은 키값입니다.', 'errorCode': '320010'}}
```


