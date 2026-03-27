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
    var State = { "ccdsoftCamera": {}, "sky6RASCOMTele": {} };
}

// We cannot sleep in ES5, so we return a simulated runtime to the host, which can then simulate the wait if desired.
var SimulatedRuntime = 0;

//
// The ccdsoftCamera interface.
//
var _ccdsoftCamera = {
    Abort: function () {
    },
    Connect: function () {
    },
    Disconnect: function () {
    },
    TakeImage: function () {
        if (this.Asynchronous === 0) {
            SimulatedRuntime += this.ExposureTime * 1000;
        }
    },
};
var _ccdsoftCameraDefaults = {
    AutoSavePrefix: "",
    Asynchronous: 0,
    Frame: 0,
    ExposureTime: 10.0,
    BinX: 0,
    BinY: 0,
    Status: {},
};

// Create the ccdsoftCamera object, restoring fields from the input state.
var ccdsoftCamera = Object.assign(Object.create(_ccdsoftCamera), _ccdsoftCameraDefaults, State.ccdsoftCamera);

//
// The sky6RASCOMTele interface.
//
var _sky6RASCOMTele = {
    Abort: function () {
    },
    ConnectAndDoNotUnpark: function () {
        this.IsConnected = true;
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
    },
    SetTracking: function (a, b, c, d) {
        if (!this.IsConnected || this.IsParked()) throw false;
        if (this.Asynchronous === 0) {
            SimulatedRuntime += 500;
        }
        this.IsTracking = 1;
        this.LastSlewError = 0;
    },
    SlewToRaDec: function (ra, dec, name) {
        if (!this.IsConnected || this.IsParked()) throw false;
        if (this.Asynchronous === 0) {
            SimulatedRuntime += 1000;
        }
        this.IsTracking = 1;
        this.LastSlewError = 0;
    },
    Unpark: function () {
        if (!this.IsConnected) throw false;
        if (!this.IsParked()) return;
        if (this.Asynchronous === 0) {
            SimulatedRuntime += 1000;
        }
        this._IsParked = false;
    },
};
var _sky6RASCOMTeleDefaults = {
    _IsParked: true,
    Asynchronous: 0,
    IsConnected: false,
    IsTracking: 0,
    LastSlewError: 0,
    dAlt: 0.0,
    dAz: 0.0,
    dDec: 0.0,
    dDecTrackingRate: 0.0,
    dRa: 0.0,
    dRaTrackingRate: 0.0,
};

// Create the sky6RASCOMTele object, restoring fields from the input state.
var sky6RASCOMTele = Object.assign(Object.create(_sky6RASCOMTele), _sky6RASCOMTeleDefaults, State.sky6RASCOMTele);

// This is used by user scripts to specify the payload of the response.
var Out = null;

{
//USER_SCRIPT
}

// Bundle up all info about the simulation, including the new state, for consumption by the host.
var Simulation = {
    state: {
        "ccdsoftCamera": ccdsoftCamera,
        "sky6RASCOMTele": sky6RASCOMTele,
    },
    events: [],  // TODO
    runtime: SimulatedRuntime,
    result: Out,
};

Simulation
