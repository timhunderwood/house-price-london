import os
from typing import Optional

import shapefile as shp
import numpy
import pandas

import matplotlib.animation
import matplotlib.ticker
import matplotlib.pyplot as plt

from pandas.plotting import register_matplotlib_converters
from matplotlib.patches import Polygon
from matplotlib.collections import PatchCollection

register_matplotlib_converters()


class MapView:
    """Class to plot the view (map and line plots etc.)."""

    def __init__(self, shp_file_name: str):
        """Instantiate the MapView.

        :param path:
        """
        self._shp_path = os.path.join(os.getcwd(), "..", "data", shp_file_name)

        self.shape_reader = shp.Reader(self._shp_path)
        figsize = 3 * numpy.array([2, 3])
        self.fig = plt.figure(constrained_layout=True, figsize=figsize)
        gs = self.fig.add_gridspec(3, 2)
        self.map_ax = self.fig.add_subplot(gs[0:2, :])
        self.plot_ax = self.fig.add_subplot(gs[2, :])

        self.borough_to_plot_dict = {}
        self.boroughs = []
        self.patches = []
        self.patch_collection = None
        self.text_on_axis = None
        self.disclaimer = None
        self._borough_name_mappings = {
            "WESTMINSTER": "CITY OF WESTMINSTER"
        }  # align between names in house price data and in shape file

    def initial_draw(self) -> None:
        """Initial configuration and drawing of plots (called once at beginning of animation)."""
        self._loop_over_shape_file_and_create_polygons()
        self.sort_patches_and_boroughs()
        self._add_patches_to_collection_and_axis()
        self._configure_axis()
        self._create_initial_color_bar()

    def _loop_over_shape_file_and_create_polygons(self) -> None:
        """Loops over the records in the shape files (boroughs) and appends them to lists for drawing."""
        for shape in self.shape_reader.shapeRecords():
            borough = shape.record[0].upper()
            if borough in self._borough_name_mappings:
                borough = self._borough_name_mappings[borough]
            xy = numpy.array(shape.shape.points)
            polygon = Polygon(xy, False)
            self.patches.append(polygon)
            self.borough_to_plot_dict[borough] = polygon
            self.boroughs.append(borough)

    def _configure_axis(self) -> None:
        """Configure the plot axes."""
        self.map_ax.set_xlim(left=5.025e5, right=5.625e5)
        self.map_ax.set_ylim(bottom=1.55e5, top=2.025e5)
        self.map_ax.set_aspect("equal")
        self.map_ax.get_xaxis().set_visible(False)
        self.map_ax.get_yaxis().set_visible(False)
        self.plot_ax.set_ylim(bottom=0, top=7.5e5)
        self.plot_ax.set_xlim(
            left=pandas.to_datetime("1995-01-01"),
            right=pandas.to_datetime("2020-01-01"),
        )
        self.line, = self.plot_ax.plot([], [], "-")
        self.plot_ax.yaxis.set_major_formatter(
            matplotlib.ticker.FuncFormatter(lambda x, p: "{}k".format(int(x // 1000)))
        )

    def _add_patches_to_collection_and_axis(self) -> None:
        """Add the patches (borough boundaries) to a PatchCollection and attach to the axes plot."""
        self.patch_collection = PatchCollection(self.patches, cmap="plasma")
        self.map_ax.add_collection(self.patch_collection)

    def sort_patches_and_boroughs(self) -> None:
        """Make sure patches (boroughs) are sorted alphebetically so they match DataLoader's order."""
        zipped = zip(self.boroughs, self.patches)
        self.patches = [
            patch for borough, patch in sorted(zipped, key=lambda pair: pair[0])
        ]
        self.boroughs = sorted(self.boroughs)

    def _create_initial_color_bar(self) -> None:
        """Create a colour bar for the cmap plot"""
        array = numpy.array((len(self.patches) - 1) * [0] + [1e6])
        self.patch_collection.set_array(array)
        colorbar = self.fig.colorbar(self.patch_collection, ax=self.map_ax)
        self.patch_collection.set_clim(0, 1e6)
        colorbar.vmin = 0
        colorbar.vmax = 1e6
        colorbar.set_ticks(numpy.arange(0, 1.1e6, 1e5))
        colorbar.ax.yaxis.set_major_formatter(
            matplotlib.ticker.FuncFormatter(lambda x, p: "{}k".format(int(x // 1000)))
        )

    def draw_text_on_axis(self, year: int, month: int) -> None:
        """Write the year month as text on the axes and disclaimer text.

        :param year:
        :param month:
        :return:
        """
        if self.text_on_axis is not None:
            self.text_on_axis.remove()
        self.text_on_axis = self.map_ax.text(
            0.0,
            1.0,
            "{}-{}".format(year, month),
            transform=self.map_ax.transAxes,
            fontsize=24,
        )
        if self.disclaimer is not None:
            self.disclaimer.remove()
        self.disclaimer = self.plot_ax.text(
            0.1,
            -0.5,
            "Contains National Statistics data © Crown copyright and database right [2015]\n"
            "Contains Ordnance Survey data © Crown copyright and database right [2015]\n"
            "Contains HM Land Registry data © Crown copyright and database right 2019.\n"
            " This data is licensed under the Open Government Licence v3.0.",
            transform=self.plot_ax.transAxes,
            fontsize=4,
        )

    def show(self) -> None:
        plt.show()

    def set_colors_for_patches(self, colors_array: numpy.ndarray) -> None:
        """Set the values to each patch (gets mapped to color in cmap plot).

        :param numpy.array colors_array: same length as self.patches
        """
        self.patch_collection.set_array(colors_array)

    def animate(self, update_function, frames) -> None:
        """Animate the plot using update_function for a specified number of frames.

        :param update_function: function to be called to update map view (using updated data)
        :param frames:  number of frames
        """
        Writer = matplotlib.animation.writers["ffmpeg"]
        writer = Writer(fps=15, metadata=dict(artist="Tim"))

        self.animation = matplotlib.animation.FuncAnimation(
            self.fig,
            update_function,
            init_func=self.initial_draw,
            interval=50,
            frames=frames,
            repeat=False,
        )
        self.animation.save("mean_prices.mp4", writer=writer)

    def plot_line(self, plot_x_data: numpy.ndarray, plot_y_data: numpy.ndarray):
        """Update the line plot."""
        self.line.set_data(plot_x_data, plot_y_data)


if __name__ == "__main__":
    map_view = MapView()
    map_view.initial_draw()
    plt.show()
