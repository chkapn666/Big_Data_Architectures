import time
import requests
import concurrent.futures

img_urls = [
    'https://images.unsplash.com/photo-1639074430765-d87212afbfb9',
    'https://images.unsplash.com/photo-1639052558054-12ab3029d10d',
    'https://images.unsplash.com/photo-1639048759498-22f2068d00ff',
    'https://images.unsplash.com/photo-1638990661686-5f5975dac8be',
    'https://images.unsplash.com/photo-1639080572734-1380055a656f',
    'https://images.unsplash.com/photo-1639074916237-5d24bc87e0d5',
    'https://images.unsplash.com/photo-1638984849659-80ae303fb4a0',
    'https://images.unsplash.com/photo-1639080921688-a7cb105017b4',
    'https://images.unsplash.com/photo-1639091320907-e4315636b536',
    'https://images.unsplash.com/photo-1639049911589-e9d75b983eff'
]

start = time.perf_counter()

def download_img(img_url):
    img_bytes = requests.get(img_url).content
    img_name = img_url.split('/')[3]
    img_name = f'images/{img_name}.jpg'
    with open(img_name, 'wb') as img_file:
        img_file.write(img_bytes)
        print(f'{img_name} downloaded.')

with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(download_img, img_urls)

finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)} second(s)')