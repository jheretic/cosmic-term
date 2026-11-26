# cosmic-term-plus
This is a soft fork of the COSMIC terminal emulator, which adds the following features:
- Quake-like dropdown functionality (run with `--dropdown` flag, bind `cosmic-term --dropdown` to a hotkey to toggle)
- Panes are inside tabs instead of tabs inside panes
- New panes optionally retain the current working directory in addition to new tabs and windows

Original README contents below:

COSMIC terminal emulator, built using [alacritty\_terminal](https://docs.rs/alacritty_terminal) that is provided by the [alacritty](https://github.com/alacritty/alacritty) project. `cosmic-term` provides bidirectional rendering and ligatures with a custom renderer based on [cosmic-text](https://github.com/pop-os/cosmic-text).

The `wgpu` feature, enabled by default, supports GPU rendering using `glyphon`
and `wgpu`. If `wgpu` is not enabled or fails to initialize, then rendering falls
back to using `softbuffer` and `tiny-skia`.

## Color Schemes

Custom color schemes can be imported from the `View -> Color schemes...` menu item.
You can find templates for color schemes in the [color-schemes](color-schemes) folder.
