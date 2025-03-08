import time
from PIL import Image, ImageFilter
import concurrent.futures

import requests
import concurrent.futures

img_names = [
    'photo-1639074430765-d87212afbfb9.jpg',
    'photo-1639052558054-12ab3029d10d.jpg',
    'photo-1639048759498-22f2068d00ff.jpg',
    'photo-1638990661686-5f5975dac8be.jpg',
    'photo-1639080572734-1380055a656f.jpg',
    'photo-1639074916237-5d24bc87e0d5.jpg',
    'photo-1638984849659-80ae303fb4a0.jpg',
    'photo-1639080921688-a7cb105017b4.jpg',
    'photo-1639091320907-e4315636b536.jpg',
    'photo-1639049911589-e9d75b983eff.jpg'
]

size = (1200, 1200)
start = time.perf_counter()

def process_image(img_name):
    img = Image.open(f'images/{img_name}')
    img = img.filter(ImageFilter.GaussianBlur(15))
    img.thumbnail(size)
    img.save(f'processed/{img_name}')
    print(f'{img_name} processed.')

with concurrent.futures.ProcessPoolExecutor() as executor:
    executor.map(process_image, img_names)

finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)} second(s)')