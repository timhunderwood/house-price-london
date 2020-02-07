import borough_map.controller

if __name__ == "__main__":
    controller = borough_map.controller.Controller(
        raw_price_paid_file_name="pp-complete.csv.gz",
        shp_file_name="London_Borough_Excluding_MHW.shp",
        start_year=1995,
        end_year=2019,
        end_month=11,
    )
    controller.animate()
