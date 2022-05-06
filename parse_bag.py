import asyncio
import os.path
from pyppeteer import launch
from proxy_authentication import usr, pwd, ip_port
from fake_useragent import UserAgent
import urllib.request
import pandas as pd
import os


ua = UserAgent()


async def save_card_links_to_txt():
    browser = await launch(
        {'args': ['--no-sandbox', f'--proxy-server={ip_port}'], 'headless': True}
    )
    page = await browser.newPage()
    await page.setUserAgent(userAgent=ua.chrome)
    await page.authenticate({'username': usr, 'password': pwd})
    await page.goto('https://www.ozon.ru/category/sumki-na-plecho-zhenskie-17002/?category_was_predicted=true&from_global=true&text=%D1%81%D1%83%D0%BC%D0%BA%D0%B0+%D0%B6%D0%B5%D0%BD%D1%81%D0%BA%D0%B0%D1%8F+%D1%87%D0%B5%D1%80%D0%B5%D0%B7+%D0%BF%D0%BB%D0%B5%D1%87%D0%BE')

    cards = await page.xpath('//*[@class="iq6 qi6"]')
    card_links = []
    card_num = 1
    for card in cards[:6]:
        print(f'current card / all of cards: {card_num}/{len(cards)}')
        try:
            await card.querySelector('a.yc5')
            await page.waitForSelector('a.yc5')
            hrefs = await card.querySelector('a.yc5')

            link_raw = await page.evaluate("element => element.getAttribute('href')", hrefs)
            link = f"https://www.ozon.ru{link_raw.rsplit('?')[0]}reviews/"
            print('card link: ', link)
            card_links.append(link)
        except Exception:
            continue
        card_num += 1

    if not os.path.exists(f'data/'):
        os.mkdir(f'data/')

    with open('data/bag_links.txt', 'w') as file:
        for card_link in card_links:
            file.write(f'{card_link}\n')
    print('Card links with comments has been recorded to "data/bag_links.txt".')

    await browser.close()


async def get_comment_from_card_links(card_links_path):
    browser = await launch({'args': ['--no-sandbox', f'--proxy-server={ip_port}'], 'headless': True})
    page = await browser.newPage()
    await page.setUserAgent(userAgent=ua.chrome)
    await page.authenticate({'username': usr, 'password': pwd})
    with open(card_links_path) as file:
        card_links = file.read().splitlines()
    card = 1
    all_cards = len(card_links)
    for card_link in card_links[:6]:
        card_name = card_link.split('/')[-3]
        print(f'current card / all of cards     {card}/{all_cards}')
        #  card pagination
        page_num = 1
        bag_list = []
        while True:
            await page.goto(f'{card_link}?page={page_num}')
            if await page.xpath('//*[@class="pz8"]'):
                await page.waitForXPath('//*[@class="pz8"]')
                post_blocks = await page.xpath('//*[@class="pz8"]')

                for post_block in post_blocks:
                    product_name = await page.Jeval('a.t8g.r8i', 'element => element.innerText')
                    user_name = await post_block.Jeval('span.o6x', 'element => element.innerText')
                    if user_name == 'Пользователь предпочёл скрыть свои данные':
                        user_name = 'No Name'
                    else:
                        user_name = user_name
                    post_date = await post_block.Jeval('div.pp6', 'element => element.innerText')
                    post_stars = await post_block.Jeval('div.ui-ba8', 'element => element.getAttribute("style")')
                    # user_comment = await post_block.Jeval('.pq', 'element => element.textContent')
                    if await post_block.JJ('.pq'):
                        user_comment = [
                            comment.replace('\n', '').replace('\xa0', '') for comment in
                            await post_block.JJeval('.pq', '(list) => list.map((element) => element.textContent)')
                        ]
                    else:
                        user_comment = 'n/a'
                    if await post_block.JJ('.o1z.ui-r4'):
                        images = await post_block.JJeval(
                            '.o1z.ui-r4', '(list) => list.map((element) => element.getAttribute("src"))'
                        )
                        #  save images to directory
                        if not os.path.exists(f'data/image/'):
                            os.mkdir(f'data/image/')

                        for image in images:
                            img = image.split('/')[-1].rsplit('.')[0]
                            date = post_date.replace(' ', '-')
                            pr_name = product_name.replace(' ', '-')
                            usr_name = user_name.replace(' ', '-')
                            urllib.request.urlretrieve(image, f'data/image/{pr_name}-{usr_name}-{date}-{img}.jpg')
                    else:
                        images = 'n/a'

                    bag = {
                        'user name': user_name,
                        'post date': post_date,
                        'post stars': post_stars.split(':')[-1].split(';')[0],
                        'user comment': user_comment,
                        'images': images
                    }
                    bag_list.append(bag)
            else:
                break
            print(f'page num:     {page_num}')
            page_num += 1
        card += 1
        all_cards -= 1
        print('card link: ', card_link)

        if not os.path.exists(f'data/csv_files/'):
            os.mkdir(f'data/csv_files/')

        df = pd.DataFrame(bag_list)
        df.to_csv(f'data/csv_files/{card_name}.csv', index=False, encoding='utf-8')
    print('The scraping of cards has been completed and saved to csv')
    await browser.close()


async def main():
    await save_card_links_to_txt()
    await get_comment_from_card_links('data/bag_links.txt')


asyncio.get_event_loop().run_until_complete(main())
