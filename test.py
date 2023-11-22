import cianparser

if __name__ == "__main__":
    start_page = 1
    for i in range(6):
        data = cianparser.parse(
            deal_type="sale",
            accommodation_type="flat",
            location="Москва",
            rooms="all",
            start_page=start_page,
            end_page=start_page + 10,
            is_saving_csv=True,
            is_express_mode=True
        )
        start_page += 10
