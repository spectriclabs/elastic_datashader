import argparse
import urllib.request
import math
import os
import time


def build_cache(base_url, start_level=0, end_level=10):
    num_tiles = 0
    for z in range(start_level, end_level+1):
        for x in range(int(math.pow(2,z))):
            for y in range(int(math.pow(2,z))):
                url = base_url+"/%s/%s/%s.png"%(z,x,y)
                with urllib.request.urlopen(url):
                    num_tiles += 1
                    print(url)
    return num_tiles




if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Request TMS tiles to build up the cache')
    parser.add_argument('base_url', type=str, help='url to request minus the tile z/x/y information')
    parser.add_argument('-l', '--end_level', type=int, default=10, help='Max level to decend to')
    parser.add_argument('-s', '--start_level', type=int, default=0, help='Min level to decend to')


    args = parser.parse_args()
    
    s1 = time.time()
    num_tiles = build_cache(args.base_url, start_level=args.start_level, end_level=args.end_level)
    s2 = time.time()
    print("Took %s sec for %s tiles (%s s/tile)"  % ((s2-s1), num_tiles, (s2-s1)/num_tiles))