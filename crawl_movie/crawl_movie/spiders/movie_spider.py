# scrapy crawl movies
import scrapy
import mysql.connector
import re

class MovieSpider(scrapy.Spider):
    name = "movies"

    def __init__(self):
        mydb = mysql.connector.connect(
                host="localhost",
                user="movieuser",
                password="password",
                database="moviedatabase"
        )
        mycursor = mydb.cursor()
        self.mycursor = mycursor
        self.mydb = mydb

    def create_table(self, table):
        fields = "(id INT AUTO_INCREMENT PRIMARY KEY, \
                    name VARCHAR(255), \
                    year YEAR, \
                    trailer_url VARCHAR(255), \
                    ratings DECIMAL(4, 2), \
                    votes INT, \
                    watched BIT(1))"
        self.mycursor.execute("CREATE TABLE " + table + fields)
        self.mydb.commit()

    # create table for each genre
    def start_requests(self):
        base_urls = ['https://www.imdb.com/feature/genre']
        for url in base_urls:
            yield scrapy.Request(url=url, callback=self.parse_genre_url)

    # parse genres and urls of genre pages
    def parse_genre_url(self, response):
        for a in response.xpath('//div[@class="ninja_left"]' + '/div/'*4 + 'a'):
            genre_name = a.css('img.pri_image::attr(title)').get().lower().replace("-","")
            self.create_table(genre_name)
            genre_url = a.css('::attr(href)').get()
            yield scrapy.Request(url=genre_url, callback=self.parse_genre_content, meta={'genre_name': genre_name})

    # parse movie name, year, rating, and votes
    def parse_genre_content(self, response):
        genre_name = response.meta['genre_name']
        for div in response.xpath('//div[@class="lister-item-content"]'):
            rating = div.css('div.inline-block.ratings-imdb-rating::attr(data-value)').get()
            votes = div.css('p.sort-num_votes-visible span[name="nv"]::attr(data-value)').get()
            trailer = 'https://www.imdb.com/' + div.css('h3.lister-item-header a::attr(href)').get()
            name = div.css('h3.lister-item-header a::text').get().encode('ascii', "replace").decode("ascii")
            year = div.css('h3.lister-item-header span.lister-item-year.text-muted.unbold::text').get()
            try:
                year = year.encode('ascii', "replace").decode("ascii")
                year = re.findall(r'\d{4}', year)[-1]
            except:
                year = 0
            sql = "INSERT INTO " + genre_name + " (name, year, trailer_url, ratings, votes) VALUES (%s, %s, %s, %s, %s)"
            self.mycursor.execute(sql, (name, year, trailer, rating, votes))
            self.mydb.commit()
            print("!!!Save data :", genre_name, name, year, trailer, rating, votes)
        next_url = response.css('a.lister-page-next.next-page::attr(href)').get()
        if next_url != None:
            yield response.follow(next_url, callback=self.parse_genre_content, meta={'genre_name': genre_name})
        
