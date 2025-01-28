package telemetry

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"fleet-telemetry-consumer/db"
	"fleet-telemetry-consumer/models"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/teslamotors/fleet-telemetry/protos"
	"google.golang.org/protobuf/proto"
)

type Consumer struct {
	consumer *kafka.Consumer
	topic    string
	running  bool
}

func NewConsumer(brokers, topic, groupID string) (*Consumer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %v", err)
	}

	return &Consumer{
		consumer: consumer,
		topic:    topic,
	}, nil
}

func (c *Consumer) Start() error {
	if err := c.consumer.Subscribe(c.topic, nil); err != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %v", c.topic, err)
	}

	log.Printf("Subscribed to topic: %s", c.topic)
	c.running = true

	// Handle graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Handle shutdown in a separate goroutine
	go func() {
		sig := <-sigchan
		log.Printf("Caught signal %v, initiating shutdown...", sig)
		c.running = false
		signal.Stop(sigchan)
		close(sigchan)
	}()

	// Start consuming messages
	log.Println("Starting to consume messages...")
	for c.running {
		msg, err := c.consumer.ReadMessage(100) // Add 100ms timeout
		if err != nil {
			if err.(kafka.Error).Code() == kafka.ErrTimedOut {
				if !c.running {
					break
				}
				continue
			}
			log.Printf("Error reading message: %v", err)
			continue
		}

		// Decode and handle the message
		if err := c.handleMessage(msg); err != nil {
			log.Printf("Error handling message: %v", err)
		}
	}

	log.Println("Shutting down consumer...")
	return c.consumer.Close()
}

func (c *Consumer) handleMessage(msg *kafka.Message) error {
	// The message value should be raw protobuf data
	// Unmarshal the protobuf message directly
	payload := &protos.Payload{}
	if err := proto.Unmarshal(msg.Value, payload); err != nil {
		return fmt.Errorf("failed to unmarshal protobuf: %v", err)
	}

	// Log the decoded message
	log.Printf("Received payload: %+v", payload)

	// Store each data point in the payload
	for _, data := range payload.Data {
		telemetryData := &models.TelemetryData{
			VIN:       payload.Vin,
			Key:       data.Key.String(),
			CreatedAt: time.Unix(payload.CreatedAt.Seconds, int64(payload.CreatedAt.Nanos)),
		}

		// Set the appropriate value based on the type
		if data.Value != nil {
			switch v := data.Value.Value.(type) {
			case *protos.Value_StringValue:
				str := v.StringValue
				telemetryData.StringValue = &str
			case *protos.Value_IntValue:
				i32 := int32(v.IntValue)
				telemetryData.IntValue = &i32
			case *protos.Value_LongValue:
				i64 := v.LongValue
				telemetryData.LongValue = &i64
			case *protos.Value_FloatValue:
				f32 := v.FloatValue
				telemetryData.FloatValue = &f32
			case *protos.Value_DoubleValue:
				f64 := v.DoubleValue
				telemetryData.DoubleValue = &f64
			case *protos.Value_LocationValue:
				if v.LocationValue != nil {
					lat := v.LocationValue.Latitude
					lon := v.LocationValue.Longitude
					telemetryData.Latitude = &lat
					telemetryData.Longitude = &lon
				}
			case *protos.Value_ChargingValue:
				if v.ChargingValue != protos.ChargingState_ChargeStateUnknown {
					state := v.ChargingValue.String()
					telemetryData.ChargingState = &state
				}
			case *protos.Value_ShiftStateValue:
				if v.ShiftStateValue != protos.ShiftState_ShiftStateUnknown {
					state := v.ShiftStateValue.String()
					telemetryData.ShiftState = &state
				}
			case *protos.Value_LaneAssistLevelValue:
				if v.LaneAssistLevelValue != protos.LaneAssistLevel_LaneAssistLevelUnknown {
					level := v.LaneAssistLevelValue.String()
					telemetryData.LaneAssistLevel = &level
				}
			case *protos.Value_ScheduledChargingModeValue:
				if v.ScheduledChargingModeValue != protos.ScheduledChargingModeValue_ScheduledChargingModeUnknown {
					mode := v.ScheduledChargingModeValue.String()
					telemetryData.ScheduledChargingMode = &mode
				}
			case *protos.Value_SentryModeStateValue:
				if v.SentryModeStateValue != protos.SentryModeState_SentryModeStateUnknown {
					state := v.SentryModeStateValue.String()
					telemetryData.SentryModeState = &state
				}
			case *protos.Value_SpeedAssistLevelValue:
				if v.SpeedAssistLevelValue != protos.SpeedAssistLevel_SpeedAssistLevelUnknown {
					level := v.SpeedAssistLevelValue.String()
					telemetryData.SpeedAssistLevel = &level
				}
			case *protos.Value_BmsStateValue:
				if v.BmsStateValue != protos.BMSStateValue_BMSStateUnknown {
					state := v.BmsStateValue.String()
					telemetryData.BMSState = &state
				}
			case *protos.Value_BuckleStatusValue:
				if v.BuckleStatusValue != protos.BuckleStatus_BuckleStatusUnknown {
					status := v.BuckleStatusValue.String()
					telemetryData.BuckleStatus = &status
				}
			case *protos.Value_CarTypeValue:
				if v.CarTypeValue != protos.CarTypeValue_CarTypeUnknown {
					carType := v.CarTypeValue.String()
					telemetryData.CarType = &carType
				}
			case *protos.Value_ChargePortValue:
				if v.ChargePortValue != protos.ChargePortValue_ChargePortUnknown {
					port := v.ChargePortValue.String()
					telemetryData.ChargePort = &port
				}
			case *protos.Value_ChargePortLatchValue:
				if v.ChargePortLatchValue != protos.ChargePortLatchValue_ChargePortLatchUnknown {
					latch := v.ChargePortLatchValue.String()
					telemetryData.ChargePortLatch = &latch
				}
			case *protos.Value_DriveInverterStateValue:
				if v.DriveInverterStateValue != protos.DriveInverterState_DriveInverterStateUnknown {
					state := v.DriveInverterStateValue.String()
					telemetryData.DriveInverterState = &state
				}
			case *protos.Value_HvilStatusValue:
				if v.HvilStatusValue != protos.HvilStatus_HvilStatusUnknown {
					status := v.HvilStatusValue.String()
					telemetryData.HvilStatus = &status
				}
			case *protos.Value_WindowStateValue:
				if v.WindowStateValue != protos.WindowState_WindowStateUnknown {
					state := v.WindowStateValue.String()
					telemetryData.WindowState = &state
				}
			case *protos.Value_SeatFoldPositionValue:
				if v.SeatFoldPositionValue != protos.SeatFoldPosition_SeatFoldPositionUnknown {
					position := v.SeatFoldPositionValue.String()
					telemetryData.SeatFoldPosition = &position
				}
			case *protos.Value_TractorAirStatusValue:
				if v.TractorAirStatusValue != protos.TractorAirStatus_TractorAirStatusUnknown {
					status := v.TractorAirStatusValue.String()
					telemetryData.TractorAirStatus = &status
				}
			case *protos.Value_FollowDistanceValue:
				if v.FollowDistanceValue != protos.FollowDistance_FollowDistanceUnknown {
					distance := v.FollowDistanceValue.String()
					telemetryData.FollowDistance = &distance
				}
			case *protos.Value_ForwardCollisionSensitivityValue:
				if v.ForwardCollisionSensitivityValue != protos.ForwardCollisionSensitivity_ForwardCollisionSensitivityUnknown {
					sensitivity := v.ForwardCollisionSensitivityValue.String()
					telemetryData.ForwardCollisionSensitivity = &sensitivity
				}
			case *protos.Value_GuestModeMobileAccessValue:
				if v.GuestModeMobileAccessValue != protos.GuestModeMobileAccess_GuestModeMobileAccessUnknown {
					access := v.GuestModeMobileAccessValue.String()
					telemetryData.GuestModeMobileAccess = &access
				}
			case *protos.Value_TrailerAirStatusValue:
				if v.TrailerAirStatusValue != protos.TrailerAirStatus_TrailerAirStatusUnknown {
					status := v.TrailerAirStatusValue.String()
					telemetryData.TrailerAirStatus = &status
				}
			case *protos.Value_DetailedChargeStateValue:
				if v.DetailedChargeStateValue != protos.DetailedChargeStateValue_DetailedChargeStateUnknown {
					state := v.DetailedChargeStateValue.String()
					telemetryData.DetailedChargeState = &state
				}
			case *protos.Value_HvacAutoModeValue:
				if v.HvacAutoModeValue != protos.HvacAutoModeState_HvacAutoModeStateUnknown {
					mode := v.HvacAutoModeValue.String()
					telemetryData.HvacAutoMode = &mode
				}
			case *protos.Value_ClimateKeeperModeValue:
				if v.ClimateKeeperModeValue != protos.ClimateKeeperModeState_ClimateKeeperModeStateUnknown {
					mode := v.ClimateKeeperModeValue.String()
					telemetryData.ClimateKeeperMode = &mode
				}
			case *protos.Value_HvacPowerValue:
				if v.HvacPowerValue != protos.HvacPowerState_HvacPowerStateUnknown {
					state := v.HvacPowerValue.String()
					telemetryData.HvacPowerState = &state
				}
			case *protos.Value_FastChargerValue:
				if v.FastChargerValue != protos.FastCharger_FastChargerUnknown {
					charger := v.FastChargerValue.String()
					telemetryData.FastCharger = &charger
				}
			case *protos.Value_CableTypeValue:
				if v.CableTypeValue != protos.CableType_CableTypeUnknown {
					cableType := v.CableTypeValue.String()
					telemetryData.CableType = &cableType
				}
			case *protos.Value_TonneauTentModeValue:
				if v.TonneauTentModeValue != protos.TonneauTentModeState_TonneauTentModeStateUnknown {
					mode := v.TonneauTentModeValue.String()
					telemetryData.TonneauTentMode = &mode
				}
			case *protos.Value_TonneauPositionValue:
				if v.TonneauPositionValue != protos.TonneauPositionState_TonneauPositionStateUnknown {
					position := v.TonneauPositionValue.String()
					telemetryData.TonneauPosition = &position
				}
			case *protos.Value_PowershareTypeValue:
				if v.PowershareTypeValue != protos.PowershareTypeStatus_PowershareTypeStatusUnknown {
					shareType := v.PowershareTypeValue.String()
					telemetryData.PowershareType = &shareType
				}
			case *protos.Value_PowershareStateValue:
				if v.PowershareStateValue != protos.PowershareState_PowershareStateUnknown {
					state := v.PowershareStateValue.String()
					telemetryData.PowershareState = &state
				}
			case *protos.Value_PowershareStopReasonValue:
				if v.PowershareStopReasonValue != protos.PowershareStopReasonStatus_PowershareStopReasonStatusUnknown {
					reason := v.PowershareStopReasonValue.String()
					telemetryData.PowershareStopReason = &reason
				}
			case *protos.Value_DisplayStateValue:
				if v.DisplayStateValue != protos.DisplayState_DisplayStateUnknown {
					state := v.DisplayStateValue.String()
					telemetryData.DisplayState = &state
				}
			case *protos.Value_DistanceUnitValue:
				if v.DistanceUnitValue != protos.DistanceUnit_DistanceUnitUnknown {
					unit := v.DistanceUnitValue.String()
					telemetryData.DistanceUnit = &unit
				}
			case *protos.Value_TemperatureUnitValue:
				if v.TemperatureUnitValue != protos.TemperatureUnit_TemperatureUnitUnknown {
					unit := v.TemperatureUnitValue.String()
					telemetryData.TemperatureUnit = &unit
				}
			case *protos.Value_PressureUnitValue:
				if v.PressureUnitValue != protos.PressureUnit_PressureUnitUnknown {
					unit := v.PressureUnitValue.String()
					telemetryData.PressureUnit = &unit
				}
			case *protos.Value_ChargeUnitPreferenceValue:
				if v.ChargeUnitPreferenceValue != protos.ChargeUnitPreference_ChargeUnitUnknown {
					unit := v.ChargeUnitPreferenceValue.String()
					telemetryData.ChargeUnitPreference = &unit
				}
			}
		}

		// Store in database
		if err := db.StoreTelemetryData(telemetryData); err != nil {
			log.Printf("Error storing telemetry data: %v", err)
		}
	}

	return nil
}
