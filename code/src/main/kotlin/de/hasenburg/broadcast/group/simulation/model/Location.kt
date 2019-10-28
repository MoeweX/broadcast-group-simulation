package de.hasenburg.broadcast.group.simulation.model

import org.apache.logging.log4j.LogManager
import org.locationtech.spatial4j.context.jts.JtsSpatialContext.GEO
import org.locationtech.spatial4j.distance.DistanceUtils.DEG_TO_KM
import org.locationtech.spatial4j.distance.DistanceUtils.KM_TO_DEG
import org.locationtech.spatial4j.shape.Point

private val logger = LogManager.getLogger()

data class Location(val lat: Double, val lon: Double) {

    /**
     * Latitude value of [Point] is Y
     * Longitude value of [Point] is X
     */
    val point: Point = GEO.shapeFactory.pointXY(lon, lat)

    /**
     * Distance between this location and the given one, as determined by the Haversine formula, in km
     *
     * [toL] - the other location
     * @return distance in km to other location
     */
    fun distanceKmTo(toL: Location): Double {
        return GEO.distCalc.distance(point, toL.point) * DEG_TO_KM
    }

}

/**
 * [distance] - distance from starting location in km
 * [direction] - direction (0 - 360)
 */
fun Location.otherInDistance(distance: Double, direction: Double) : Location {
    val result = GEO.distCalc.pointOnBearing(this.point,
            distance * KM_TO_DEG,
            direction,
            GEO,
            GEO.shapeFactory.pointXY(0.0, 0.0))

    return Location(result.y, result.x)
}