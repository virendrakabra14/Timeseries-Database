# g++ -std=c++17 main.cpp

# CXX = g++
# EXE = exec
# CXXSTD = -std=c++17

# .PHONY: all clean

# all: disk.o tmpfs.o
# 	$(CXX) $(CXXSTD) -o $(EXE) $^ main.cpp

# disk.o: disk.cpp disk.h tmpfs.hpp
# 	$(CXX) $(CXXSTD) -c $< -o $@

# tmpfs.o: tmpfs.cpp tmpfs.hpp
# 	$(CXX) $(CXXSTD) -c $< -o $@

# clean:
# 	rm -f *.o $(EXE)

all:
	g++ main.cpp -std=c++17 -o exec

backend_only:
	g++ main.cpp -std=c++17 -D__ONLYBACKEND__ -o exec

clean:
	rm exec