.PHONY: build
build-base:
	docker build . -t python3_crontab:latest

.PHONY: build-script
build-script:
	docker build ./recomendaciones -t implementacion_recomendaciones_det-sag:latest

.PHONY: run
run:
	docker run -e TZ=America/Santiago -d implementacion_recomendaciones_det-sag:latest

.PHONY: clean-image
clean-image:
	docker rmi implementacion_recomendaciones:latest