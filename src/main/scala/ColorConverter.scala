import java.awt.Color

class ColorConverter {
  def HSLAtoRGBA(hue:Float, saturation:Float, luminance:Float, alpha:Float):Color = {
    if (hue <0.0f || hue > 360.0f)
    {
      throw new IllegalArgumentException("Hue outside of expected range (0-360)" )
    }
    if (saturation <0.0f || saturation > 100.0f)
    {
      throw new IllegalArgumentException("Saturation outside of expected range (0-100)" )
    }

    if (luminance <0.0f || luminance > 100.0f)
    {
      throw new IllegalArgumentException("Luminance outside of expected range (0-100)" )
    }

    if (alpha <0.0f || alpha > 100.0f)
    {
      throw new IllegalArgumentException("Alpha outside of expected range (0-1)" )
    }

    val hueN: Float = (hue % 360.0f)/360f
    val saturationN = saturation / 100.0f
    val luminanceN = luminance / 100.0f
    val alphaN = alpha / 100.0f

    val q: Float = if (luminanceN < 0.5) luminanceN * (1 + saturationN) else (luminanceN + saturationN) - (saturationN * luminanceN)
    val p: Float = 2 * luminanceN - q

    val redN: Float = matchToRange(convertToRGB(p, q, hueN + (1.0f / 3.0f)))
    val greenN: Float = matchToRange(convertToRGB(p, q, hueN))
    val blueN: Float = matchToRange(convertToRGB(p, q, hueN - (1.0f / 3.0f)))

    new Color(redN, greenN, blueN, alphaN)
  }

  private def matchToRange(value: Float): Float = value match {
    case value if value < 0.0f => 0.0f
    case value if value > 1.0f => 1.0f
    case _ => value
  }

  private def convertToRGB(p: Float, q: Float, h: Float):Float = {
    val h2 = if (h < 0) h + 1 else if (h > 1) h - 1 else h

    if (6.0f * h2 < 1) {
      p + ((q - p) * 6.0f * h2)
    } else if (2.0f * h2 < 1){
      q
    } else if (3.0f * h2 < 2) {
      p + ( (q - p) * 6.0f * ((2.0f / 3.0f) - h2) )
    } else {
      p
    }
  }
}
