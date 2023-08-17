all: client-phase1 client-phase2 client-phase3 client-phase4 client-phase5

client-phase1: client-phase1.cpp
	g++ client-phase1.cpp -lpthread -lcrypto -o client-phase1
client-phase2: client-phase2.cpp
	g++ client-phase2.cpp -lpthread -lcrypto -o client-phase2
client-phase3: client-phase3.cpp
	g++ client-phase3.cpp -lpthread -lcrypto -o client-phase3
client-phase4: client-phase4.cpp
	g++ client-phase4.cpp -lpthread -lcrypto -o client-phase4
client-phase5: client-phase5.cpp
	g++ client-phase5.cpp -lpthread -lcrypto -o client-phase5

clean:
	rm -f client-phase1 client-phase2 client-phase3 client-phase4 client-phase5