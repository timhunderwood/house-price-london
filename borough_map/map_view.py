import shapefile as shp
import matplotlib.pyplot as plt
import os
import numpy as np
from matplotlib.patches import Polygon
from matplotlib.collections import PatchCollection
import matplotlib
import matplotlib.animation
import matplotlib.ticker
import pandas
from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()


class MapView(object):
    def __init__(self, path=None):

        if path is None:
            self.shp_path = os.path.join(
                os.getcwd(), "..", "data", "London_Borough_Excluding_MHW.shp"
            )
        else:
            self.shp_path = path

        self.shape_reader = shp.Reader(self.shp_path)

        self.fig = plt.figure(constrained_layout=True, figsize=(3 * 2, 3 * 3))
        gs = self.fig.add_gridspec(3, 2)
        self.map_ax = self.fig.add_subplot(gs[0:2, :])
        self.plot_ax = self.fig.add_subplot(gs[2, :])

        # self.fig, (self.map_ax, self.plot_ax) = plt.subplots(nrows=2)
        self.borough_to_plot_dict = {}
        self.boroughs = []
        self.patches = []
        self.patch_collection = None
        self.text_on_axis = None
        self.disclaimer = None

    def initial_draw(self):
        """
        Initially draw all plots on the map

        :return:
        """
        self._loop_over_shape_file_and_create_polygons()
        self.sort_patches_and_boroughs()
        self._add_patches_to_collection_and_axis()
        self._configure_axis()
        self._create_initial_color_bar()

    def _loop_over_shape_file_and_create_polygons(self):
        for shape in self.shape_reader.shapeRecords():
            borough = shape.record[0].upper()
            xy = np.array(shape.shape.points)
            polygon = Polygon(xy, False)
            self.patches.append(polygon)
            self.borough_to_plot_dict[borough] = polygon
            self.boroughs.append(borough)

    def _configure_axis(self):
        self.map_ax.set_xlim(left=5e5, right=5.6e5)
        self.map_ax.set_ylim(bottom=1.5e5, top=2.2e5)
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


    def _add_patches_to_collection_and_axis(self):
        self.patch_collection = PatchCollection(self.patches, cmap="inferno")
        self.map_ax.add_collection(self.patch_collection)

    def sort_patches_and_boroughs(self):
        zipped = zip(self.boroughs, self.patches)
        self.patches = [
            patch for borough, patch in sorted(zipped, key=lambda pair: pair[0])
        ]
        self.boroughs = sorted(self.boroughs)

    def _create_initial_color_bar(self):
        array = np.array((len(self.patches) - 1) * [0] + [1e6])
        self.patch_collection.set_array(array)
        colorbar = self.fig.colorbar(self.patch_collection, ax=self.map_ax)
        self.patch_collection.set_clim(0, 1e6)
        colorbar.vmin = 0
        colorbar.vmax = 1e6
        colorbar.set_ticks(np.arange(0, 1.1e6, 1e5))
        colorbar.ax.yaxis.set_major_formatter(
            matplotlib.ticker.FuncFormatter(lambda x, p: "{}k".format(int(x // 1000)))
        )

    def draw_year_month_on_axis(self, year, month):
        """

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

    def show(self):
        plt.show()

    def set_colors_for_patches(self, colors_array):
        """

        :param np.array colors_array: same length as self.patches
        :return:
        """
        self.patch_collection.set_array(colors_array)

    def animate(self, update_function, frames):
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
        # plt.show()

    def plot_line(self, plot_x_data, plot_y_data):
        self.line.set_data(plot_x_data, plot_y_data)


if __name__ == "__main__":
    map_view = MapView()
    map_view.initial_draw()
    plt.show()
