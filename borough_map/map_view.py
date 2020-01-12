import shapefile as shp
import matplotlib.pyplot as plt
import os
import numpy as np
from matplotlib.patches import Polygon
from matplotlib.collections import PatchCollection
import matplotlib
import matplotlib.animation


class MapView(object):
    def __init__(self, path=None):

        if path is None:
            self.shp_path = os.path.join(
                os.getcwd(), "..", "data", "London_Borough_Excluding_MHW.shp"
            )
        else:
            self.shp_path = path

        self.shape_reader = shp.Reader(self.shp_path)
        self.fig, self.ax = plt.subplots()
        self.borough_to_plot_dict = {}
        self.boroughs = []
        self.patches = []
        self.patch_collection = None
        self.text_on_axis = None

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
        self.ax.set_xlim(left=5e5, right=5.6e5)
        self.ax.set_ylim(bottom=1.5e5, top=2.2e5)
        self.ax.set_aspect("equal")
        self.ax.get_xaxis().set_visible(False)
        self.ax.get_yaxis().set_visible(False)

    def _add_patches_to_collection_and_axis(self):
        self.patch_collection = PatchCollection(self.patches, cmap="inferno")
        self.ax.add_collection(self.patch_collection)

    def sort_patches_and_boroughs(self):
        zipped = zip(self.boroughs, self.patches)
        self.patches = [
            patch for borough, patch in sorted(zipped, key=lambda pair: pair[0])
        ]
        self.boroughs = sorted(self.boroughs)

    def _create_initial_color_bar(self):
        array = np.array((len(self.patches) - 1) * [0] + [1e6])
        self.patch_collection.set_array(array)
        colorbar = self.fig.colorbar(self.patch_collection, ax=self.ax)
        colorbar.set_clim(0, 1e6)
        colorbar.vmin = 0
        colorbar.vmax = 1e6
        colorbar.set_ticks(np.arange(0, 1.1e6, 1e5))

    def draw_year_month_on_axis(self, year, month):
        """

        :param year:
        :param month:
        :return:
        """
        if self.text_on_axis is not None:
            self.text_on_axis.remove()
        self.text_on_axis = self.ax.text(
            0.0,
            1.0,
            "{}-{}".format(year, month),
            transform=self.ax.transAxes,
            fontsize=24,
        )

    def show(self):
        plt.show()

    def set_colors_for_patches(self, colors_array):
        """

        :param np.array colors_array: same length as self.patches
        :return:
        """
        self.patch_collection.set_array(colors_array)

    def animate(self, update_function):
        Writer = matplotlib.animation.writers["ffmpeg"]
        writer = Writer(fps=15, metadata=dict(artist="Tim"))

        self.animation = matplotlib.animation.FuncAnimation(
            self.fig,
            update_function,
            init_func=self.initial_draw,
            interval=50,
            frames=12 * 24,
            repeat=False,
        )
        self.animation.save("mean_prices.mp4", writer=writer)
        # plt.show()


if __name__ == "__main__":
    map_view = MapView()
    map_view.initial_draw()
