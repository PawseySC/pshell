PY_VERSION?=2

PY_DOCKERFILE=Dockerfile.py2env
PY_IMAGE=seanfleming/python-$(PY_VERSION)
PY_CONTAINER=py-container

MF_IMAGE=mediaflux_mflux
MF_CONTAINER=mf-container



mfenv:
	docker run --name $(MF_CONTAINER) --rm -d -p 80:80 --mac-address 00:1E:67:6B:01:C6 $(MF_IMAGE)
	touch mfenv


pyenv: $(PY_DOCKERFILE)
	docker build --tag $(PY_IMAGE) -f $(PY_DOCKERFILE) .
	echo "PY_IMAGE=$(PY_IMAGE)" > ".env"
	docker run --name $(PY_CONTAINER) --rm -dit $(PY_IMAGE)
	touch pyenv


pshell: mfclient.py pshell.py pyenv mfenv
	docker cp mfclient.py $(PY_CONTAINER):/mfclient.py
	docker cp pshell.py $(PY_CONTAINER):/pshell.py
	docker exec -ti $(PY_CONTAINER) bash



clean:
	docker stop $(MF_CONTAINER)
	rm "pyenv"
	rm "mfenv"


.PHONY: clean

