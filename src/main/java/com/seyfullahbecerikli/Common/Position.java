package com.seyfullahbecerikli.Common;

public class Position {

    private float XCoordinate;
    private float YCoordinate;

    public Position(float XCoordinate, float YCoordinate) {
        this.XCoordinate = XCoordinate;
        this.YCoordinate = YCoordinate;
    }

    public float getXCoordinate() {
        return XCoordinate;
    }

    public void setXCoordinate(float XCoordinate) {
        if (XCoordinate < 1000)
            this.XCoordinate = XCoordinate;
        else
            this.XCoordinate = 1000;
    }

    public float getYCoordinate() {
        return YCoordinate;
    }

    public void setYCoordinate(float YCoordinate) {
        if (YCoordinate < 1000)
            this.YCoordinate = YCoordinate;
        else
            this.YCoordinate = 1000;
    }



}
