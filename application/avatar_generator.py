# -*- coding: utf-8 -*-

from __future__ import (unicode_literals, absolute_import,
                        division, print_function)

"""
    Generates default avatars from a given string (such as username).

    Usage:

    >>> from avatar_generator import Avatar
    >>> photo = Avatar.generate(128, "example@sysnove.fr", "PNG")
"""

import os
import re
import time
import random
from io import BytesIO
from PIL import Image, ImageDraw, ImageFont


class AvatarGenerator(object):
    FONT_COLOR = (255, 255, 255)
    RENDER_SIZE = 300
    RESULT_SIZE = 300
    FILE_TYPE = 'PNG'
    EMOT_ICONS =  [
        '(^_^)',
        '(^.^)',
        '(-_-)',
        '(-.-)',
        '(^_-)',
        '(^.-)',
        '(\'_\')',
        '(._.)',
        '(~_~)',
        '(0_0)',
        '(>_<)'
    ]

    def __init__(self, pallete_image_path, font_path):
        self._pallete_image = Image.open(pallete_image_path)
        self._font = ImageFont.truetype(font_path, size=int(0.45 * AvatarGenerator.RENDER_SIZE))
        self._tokens_regex = re.compile(r"\w+", re.UNICODE)

    def generate(self, string):
        """
            Generates a squared avatar with random background from pallete.

            :param string: string to be used to print text
        """
        text = self._text(string)
        text_pos = self._text_position(AvatarGenerator.RENDER_SIZE, text)
        image = self._background_image(AvatarGenerator.RENDER_SIZE)

        draw = ImageDraw.Draw(image)
        draw.text(text_pos,
                  text,
                  fill=AvatarGenerator.FONT_COLOR,
                  font=self._font)
        stream = BytesIO()
        if AvatarGenerator.RENDER_SIZE != AvatarGenerator.RESULT_SIZE:
            image = image.resize((AvatarGenerator.RESULT_SIZE, AvatarGenerator.RESULT_SIZE), Image.ANTIALIAS)
        image.save(stream, format=AvatarGenerator.FILE_TYPE, optimize=True)
        return stream.getvalue()

    def _background_image(self, size):
        """
            Pick a random background image.

            :param s: Seed used by the random generator
            (same seed will produce the same color).
        """
        width, height = self._pallete_image.size

        random.seed(time.time())
        background_index = random.randint(0, width / height - 1)
        background = self._pallete_image.crop((background_index * height, 0, background_index * height + 1, height))
        return background.resize((size, size), Image.BICUBIC)

    def _text(self, string):
        """
            Returns the text to draw.
        """
        if string == None:
            return random.choice(AvatarGenerator.EMOT_ICONS)

        text = ''
        tokens = self._tokens_regex.findall(string)
        if len(tokens):
            for token in tokens[:2]:
                text += token[0].upper()
        else:
            text = random.choice(AvatarGenerator.EMOT_ICONS)
        return text

    def _text_position(self, size, text):
        """
            Returns the left-top point where the text should be positioned.
        """
        width, height = self._font.getsize(text)
        left = (size - width) / 2.0
        # I just don't know why 3 :)
        top = (size - height) / 2.0
        return left, top


if __name__ == "__main__":
    generator = AvatarGenerator('data/avatar_palette.png', 'data/SanFranciscoDisplay-Regular.ttf')
    avatar = generator.generate(u'sona blade')

