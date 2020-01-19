import itertools
import logging
from typing import Iterator, Tuple

from borough_map.data_loader import DataLoader
from borough_map.map_view import MapView

logging.basicConfig()
LOGGER = logging.getLogger(__file__)


class Controller:
    """Controls the data and maps it to the map view."""

    def __init__(self, start_year=1995, end_year=2019, end_month=11):
        """Instantiate the controller."""
        self.data_loader = DataLoader()
        self.map_view = MapView()
        self.data_loader.load_prepare_and_aggregate_data()
        self._start_year, self._end_year, self._end_month = start_year, end_year, end_month
        self._frames = (self._end_year - self._start_year) * 12 + self._end_month
        self._input_iterator = self._get_year_month_pair_iterator()

    def show(self, year: int, month: int) -> None:
        """Show the plot for a specific year, month input.

        :param year: chosen year to display
        :param month: chosen month to display
        :return:
        """
        colors = self.data_loader.get_mean_prices(year, month).values
        self.map_view.initial_draw()
        self.map_view.set_colors_for_patches(colors)
        self.map_view.show()

    def _get_year_month_pair_iterator(self,) -> Iterator[Tuple[int,int]]:
        """Iterator for year month looping stopping at a fixed month in final year.

        :return: iterator
        """
        years = range(self._start_year, self._end_year + 1)
        months = range(1, 13)
        for (year, month) in itertools.product(years, months):
            if year == self._end_year and month > self._end_month:
                raise StopIteration()
            yield (year, month)

    def _update(self, i: int) -> None:
        """Called to update the map view animation.

        Uses the data loader and the map view to update the map view
        every frame of the iteration. This is then saved to a video file
        using ffmpeg.

        :param i: frame number
        """
        year, month = next(self._input_iterator)
        colors = self.data_loader.get_median_prices(year, month).values
        self.map_view.set_colors_for_patches(colors)
        self.map_view.draw_text_on_axis(year, month)
        plot_x_data, plot_y_data = self.data_loader.get_line_data()
        self.map_view.plot_line(plot_x_data[:i], plot_y_data[:i])

    def animate(self)-> None:
        """Wrapper to call the map_view's animate method."""
        self.map_view.animate(self._update, self._frames)


if __name__ == "__main__":
    controller = Controller()
    # controller.show(1996, 1)
    controller.animate()

