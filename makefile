PY_VERSION?=2

PY_DOCKERFILE=Dockerfile.py2env
PY_IMAGE=seanfleming/python-$(PY_VERSION)
PY_CONTAINER=py-container

MF_IMAGE=mediaflux_mflux
MF_CONTAINER=mf-container
# join 2 containers via this network
MF_NET=mflux
# the server= setting in .mf_config
MF_SERVER=mflux

mfenv:
	docker network create -d bridge mflux
	docker run --name $(MF_CONTAINER) --rm -d -p 80:80 --network $(MF_NET) --network-alias $(MF_SERVER) --mac-address 00:1E:67:6B:01:C6 $(MF_IMAGE)
	touch mfenv

# TODO - for the life of me could not figure out how to convert the output of a command into a variable (eval, shell etc all don't work on OS-X)
#	docker inspect -f "{{ .NetworkSettings.IPAddress }}" $(MF_CONTAINER) > "mfenv"
#	$(eval MFLUX_IP=$(shell docker inspect -f "{{ .NetworkSettings.IPAddress }}" $$(MF_CONTAINER)))


pyenv: $(PY_DOCKERFILE) mfenv 
	docker build --tag $(PY_IMAGE) -f $(PY_DOCKERFILE) .
	@echo "PY_IMAGE=$(PY_IMAGE)" > ".env"
	docker run --name $(PY_CONTAINER) --network $(MF_NET) --rm -dit $(PY_IMAGE)
	docker cp data/.mf_config $(PY_CONTAINER):/root/.mf_config
	touch pyenv


pshell: mfclient.py pshell.py pyenv mfenv
	docker cp mfclient.py $(PY_CONTAINER):/mfclient.py
	docker cp pshell.py $(PY_CONTAINER):/pshell.py
	docker exec -ti $(PY_CONTAINER) sh -c "/pshell.py -c mflux"
#	docker exec -ti $(PY_CONTAINER) bash


clean:
	rm -f pyenv mfenv
	docker stop $(MF_CONTAINER)
	docker stop $(PY_CONTAINER)
	docker network rm mflux


.PHONY: clean pshell getopts

