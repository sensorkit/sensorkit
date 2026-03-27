!/usr/bin/bash
# Adapted from https://github.com/sdss/lvmpwi/blob/main/container/run-pwi.sh

# Start VNC server.
export DISPLAY=:0
/usr/bin/vncserver \
  -depth 24 \
  -geometry ${PWI_GEOM:-1200x900} \
  -port 5900 \
  -SecurityTypes None --I-KNOW-THIS-IS-INSECURE \
  -localhost no \
  $DISPLAY

# Start novnc.
pushd /usr/share/novnc >/dev/null
~/novnc_server --listen ${NOVNC_PORT:-6080} &
popd >/dev/null

# Run PWI4.
cd ${PWI_PATH}

while true; do
  ./run-pwi4 &
  pid=$!

  # Fullscreen the window once it comes up.
  while ! wmctrl -l | grep PlaneWave; do sleep 1; done
  wmctrl -a PlaneWave
  wmctrl -r ':ACTIVE:' -b toggle,fullscreen

  wait $pid
done
