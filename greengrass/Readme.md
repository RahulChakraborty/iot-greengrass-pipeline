sed -i 's|{{config_dir}}|/greengrass/v2|g; s|{{nucleus_component}}|aws.greengrass.Nucleus|g' /greengrass/v2/config.yaml && sudo -E java -Droot="/greengrass/v2" -Dlog.store=FILE -jar ./GreengrassInstaller/lib/Greengrass.jar --init-config /greengrass/v2/config.yaml --component-default-user ggc_user:ggc_group --setup-system-service true

sed -i 's|{{config_dir}}|/greengrass/v2|g; s|{{nucleus_component}}|aws.greengrass.Nucleus|g' /greengrass/v2/config.yaml && sudo -E java -Droot="/greengrass/v2" -Dlog.store=FILE -jar ./GreengrassInstaller/lib/Greengrass.jar --init-config /greengrass/v2/config.yaml --component-default-user ggc_user:ggc_group --setup-system-service true



scp -i "lab4-greengrass-core-ec2-key-pair.pem" greengrass-nucleus-latest.zip ec2-user@44.242.199.208:Greengrass/

scp -i "lab4-greengrass-core-ec2-key-pair.pem" GreengrassLab4Group-19a95f89468-connectionKit.zip ec2-user@44.242.199.208:Greengrass/


arn:aws:s3:::rahul-iot-lab4
s3://rahul-iot-lab4/components/