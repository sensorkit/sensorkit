// A simple TheSky simulator for dukpy.
//
// There are two forms of input to this script:
//
// - The user script, if any, should be replaced in for the USER_SCRIPT marker (with comment prefix)
// - Prior simulation state should be injected as the "State" variable
//
// The simulation output is placed into the "Simulation" variable. It consists of:
//
// - The new simulation state
// - Scheduled events (times at which the simulation should be run again, to complete simulated actions)
// - The wall clock time that the operation should have taken (note ES5 has no nonblocking sleep function)
// - The user script result as assigned to the "Out" variable
State = dukpy["State"];

// We expect the `State` variable to be set in our environment already. If it isn't, we default it here.
if (typeof State == "undefined") {
    var State = {
        "ccdsoftCamera": {},
        "sky6RASCOMTele": {},
        "sky6Dome": {},
        "OpticalTubeAssembly": {},
        "WeatherUtil": {},
        "sky6StarChart": {},
    };
}

// We cannot sleep in ES5, so we return a simulated runtime to the host, which can then simulate the wait if desired.
var SimulatedRuntime = 0;

//
// The ccdsoftCamera interface.
//
var _ccdsoftCamera = {
    Abort: function () {
        this.Status = "Ready";
    },
    Connect: function () {
        this.Status = "Ready";
        this._isConnected = true;
    },
    Disconnect: function () {
        this.Status = "Not Connected";
        this._isConnected = false;
    },
    TakeImage: function () {
        this.IsExposureComplete = 1;
        if (this.Asynchronous === 0) {
            SimulatedRuntime += this.ExposureTime * 1000;
        }
    },
    // Filter wheel
    filterWheelConnect: function () {
        this._filterWheelConnected = true;
    },
    filterWheelDisconnect: function () {
        this._filterWheelConnected = false;
    },
    filterWheelIsConnected: function () {
        return this._filterWheelConnected ? 1 : 0;
    },
    // Focuser
    focConnect: function () {
        this._focConnected = true;
    },
    focDisconnect: function () {
        this._focConnected = false;
    },
    focMoveIn: function (steps) {
        this.focPosition += steps;
    },
    focMoveOut: function (steps) {
        this.focPosition += steps;
    },
    // Rotator
    rotatorConnect: function () {
        this._rotatorConnected = true;
    },
    rotatorDisconnect: function () {
        this._rotatorConnected = false;
    },
    rotatorIsConnected: function () {
        return this._rotatorConnected ? 1 : 0;
    },
    rotatorGotoPositionAngle: function (angle) {
        this.rotatorPositionAngle = angle;
    },
};
var _ccdsoftCameraDefaults = {
    AutoSavePrefix: "",
    Asynchronous: 0,
    Frame: 0,
    ExposureTime: 10.0,
    BinX: 1,
    BinY: 1,
    Status: "Not Connected",
    Temperature: -10.0,
    TemperatureSetPoint: -10.0,
    RegulateTemperature: 0,
    SubframeLeft: 0,
    SubframeTop: 0,
    SubframeRight: 1024,
    SubframeBottom: 1024,
    IsExposureComplete: 0,
    ShutDownTemperatureRegulationOnDisconnect: 0,
    FilterIndexZeroBased: 0,
    _isConnected: false,
    _filterWheelConnected: false,
    _focConnected: false,
    focIsConnected: 0,
    focPosition: 5000.0,
    _rotatorConnected: false,
    rotatorPositionAngle: 0.0,
};

// Create the ccdsoftCamera object, restoring fields from the input state.
var ccdsoftCamera = Object.assign(Object.create(_ccdsoftCamera), _ccdsoftCameraDefaults, State.ccdsoftCamera);

// Synchronize focIsConnected with _focConnected
Object.defineProperty(ccdsoftCamera, "focIsConnected", {
    get: function () { return this._focConnected ? 1 : 0; },
    set: function (v) { this._focConnected = !!v; },
    enumerable: true,
});

//
// The ccdsoftCameraImage interface.
//
var ccdsoftCameraImage = {
    AttachToActiveImager: function () {},
    HeightInPixels: 4,
    scanLine: function (y) {
        return "100,200,300,400";
    },
    JulianDay: 2460400.5,
    FITSKeyword: function (key) {
        if (key === "BITPIX") return 16;
        return "";
    },
};

//
// The sky6RASCOMTele interface.
//
var _sky6RASCOMTele = {
    Abort: function () {
        this.IsTracking = 0;
    },
    ConnectAndDoNotUnpark: function () {
        this.IsConnected = 1;
    },
    FindHome: function () {
        if (!this.IsConnected || this.IsParked()) throw false;
        if (this.Asynchronous === 0) {
            SimulatedRuntime += 1000;
        }
    },
    GetRaDec: function () {
    },
    GetAzAlt: function () {
    },
    IsParked: function () {
        return this._IsParked;
    },
    Park: function () {
        if (!this.IsConnected) throw false;
        if (this.IsParked()) return;
        if (this.Asynchronous === 0) {
            SimulatedRuntime += 1000;
        }
        this._IsParked = true;
        this.IsTracking = 0;
    },
    ParkAndDoNotDisconnect: function () {
        this.Park();
    },
    SetTracking: function (on, b, c, d) {
        if (!this.IsConnected || this.IsParked()) throw false;
        this.IsTracking = on;
        if (b === 0) {
            this.dRaTrackingRate = c;
            this.dDecTrackingRate = d;
        }
        this.LastSlewError = 0;
    },
    SlewToRaDec: function (ra, dec, name) {
        if (!this.IsConnected || this.IsParked()) throw false;
        this.dRa = ra;
        this.dDec = dec;
        this.IsTracking = 1;
        this.IsSlewComplete = 1;
        this.LastSlewError = 0;
    },
    SlewToAzAlt: function (az, alt, name) {
        if (!this.IsConnected || this.IsParked()) throw false;
        this.dAz = az;
        this.dAlt = alt;
        this.IsSlewComplete = 1;
        this.LastSlewError = 0;
    },
    Unpark: function () {
        if (!this.IsConnected) throw false;
        if (!this.IsParked()) return;
        this._IsParked = false;
    },
};
var _sky6RASCOMTeleDefaults = {
    _IsParked: true,
    Asynchronous: 0,
    IsConnected: 0,
    IsTracking: 0,
    IsSlewComplete: 0,
    LastSlewError: 0,
    dAlt: 45.0,
    dAz: 180.0,
    dDec: 30.0,
    dDecTrackingRate: 0.0,
    dRa: 12.0,
    dRaTrackingRate: 0.0,
};

// Create the sky6RASCOMTele object, restoring fields from the input state.
var sky6RASCOMTele = Object.assign(Object.create(_sky6RASCOMTele), _sky6RASCOMTeleDefaults, State.sky6RASCOMTele);

//
// The sky6RASCOMTheSky interface (for disconnect).
//
var sky6RASCOMTheSky = {
    DisconnectTelescope: function () {
        sky6RASCOMTele.IsConnected = 0;
    },
};

//
// The sky6Dome interface.
//
var _sky6Dome = {
    Connect: function () {
        this.IsConnected = 1;
    },
    Disconnect: function () {
        this.IsConnected = 0;
    },
    Park: function () {
        this.IsParkComplete = 1;
    },
    Unpark: function () {
        this.IsUnparkComplete = 1;
        this._IsParked = false;
    },
    FindHome: function () {
        this.IsFindHomeComplete = 1;
    },
    Abort: function () {
    },
    OpenSlit: function () {
        this._slitState = 1;
        this.IsOpenComplete = 1;
    },
    CloseSlit: function () {
        this._slitState = 2;
        this.IsCloseComplete = 1;
    },
    GetAzEl: function () {
    },
    slitState: function () {
        return this._slitState;
    },
    lastError: function () {
        return 0;
    },
    IsParked: function () {
        return this._IsParked;
    },
};
var _sky6DomeDefaults = {
    IsConnected: 0,
    IsParkComplete: 0,
    IsUnparkComplete: 0,
    IsFindHomeComplete: 0,
    IsOpenComplete: 0,
    IsCloseComplete: 0,
    _slitState: 0,
    _IsParked: true,
    dAz: 180.0,
};

var sky6Dome = Object.assign(Object.create(_sky6Dome), _sky6DomeDefaults, State.sky6Dome);

//
// The OpticalTubeAssembly interface.
//
var _OpticalTubeAssembly = {
    Connect: function () {
        this.isConnected = 1;
    },
    Disconnect: function () {
        this.isConnected = 0;
    },
    startOpenMirrorCover: function () {
        this.mirrorCoverState = 1;
    },
    startCloseMirrorCover: function () {
        this.mirrorCoverState = 0;
    },
};
var _OpticalTubeAssemblyDefaults = {
    isConnected: 0,
    mirrorCoverState: 0,
};

var OpticalTubeAssembly = Object.assign(Object.create(_OpticalTubeAssembly), _OpticalTubeAssemblyDefaults, State.OpticalTubeAssembly);

//
// The WeatherUtil interface.
//
var _WeatherUtil = {
    connectWeatherStation: function () {
        this.isWeatherStationConnected = 1;
    },
    disconnectWeatherStation: function () {
        this.isWeatherStationConnected = 0;
    },
};
var _WeatherUtilDefaults = {
    isWeatherStationConnected: 0,
    goodToGo: 1,
    autoStartup: 0,
    autoShutdown: 0,
    scriptedObjectsNoGoAware: 0,
};

var WeatherUtil = Object.assign(Object.create(_WeatherUtil), _WeatherUtilDefaults, State.WeatherUtil);

//
// The sky6StarChart interface (for site location).
//
var _sky6StarChart = {
    DocumentProperty: function (prop) {
        switch (prop) {
            case 0: this.DocPropOut = this._latitude; break;   // Latitude
            case 1: this.DocPropOut = this._longitude; break;  // Longitude
            case 2: this.DocPropOut = this._timeZone; break;   // TimeZone
            case 3: this.DocPropOut = this._elevation; break;  // Elevation
        }
    },
    Find: function (name) {
    },
};
var _sky6StarChartDefaults = {
    DocPropOut: 0,
    _latitude: 32.0,
    _longitude: 110.0,
    _timeZone: -7,
    _elevation: 700.0,
};

var sky6StarChart = Object.assign(Object.create(_sky6StarChart), _sky6StarChartDefaults, State.sky6StarChart);

//
// The Raven3 interface (for TLE tracking).
//
var _Raven3 = {
    trackLEOAbort: function () {
        this.trackLEOStatus = 0;
    },
    trackLEODoCommand: function (a, b) {},
    trackLEOBegin: function () {
        this.trackLEOStatus = 6;
    },
};
var _Raven3Defaults = {
    trackLEOStatus: 0,
};

var Raven3 = Object.assign(Object.create(_Raven3), _Raven3Defaults, State.Raven3);

// This is used by user scripts to specify the payload of the response.
var Out = null;

{
//USER_SCRIPT
}

// Ensure Out is never undefined (dukpy drops undefined keys from dicts).
if (typeof Out === "undefined") {
    Out = null;
}

// Bundle up all info about the simulation, including the new state, for consumption by the host.
var Simulation = {
    state: {
        "ccdsoftCamera": ccdsoftCamera,
        "sky6RASCOMTele": sky6RASCOMTele,
        "sky6Dome": sky6Dome,
        "OpticalTubeAssembly": OpticalTubeAssembly,
        "WeatherUtil": WeatherUtil,
        "sky6StarChart": sky6StarChart,
        "Raven3": Raven3,
    },
    events: [],  // TODO
    runtime: SimulatedRuntime,
    result: Out,
};

Simulation
