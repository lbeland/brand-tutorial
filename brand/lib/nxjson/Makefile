
all: nxjson

nxjson: nxjson.c test.c nxjson.h
	gcc -O2 -g -Wall ${CFLAGS} nxjson.c test.c -o nxjson

clean:
	rm -fv nxjson

.PHONY: clean
