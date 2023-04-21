package node

const (
	IdentityNotDetermined = 0
	IdentityMaster        = 1
	IdentitySlave         = 2
	IdentityAll           = IdentityMaster | IdentitySlave
)

func (n *Pool) SwitchIdentityMasterOn() {
	n.Identity = n.Identity | IdentityMaster
}

func (n *Pool) SwitchIdentityMasterOff() {
	n.Identity = n.Identity &^ IdentityMaster
}

func (n *Pool) SwitchIdentitySlaveOn() {
	n.Identity = n.Identity | IdentitySlave
}

func (n *Pool) SwitchIdentitySlaveOff() {
	n.Identity = n.Identity &^ IdentitySlave
}

func (n *Pool) IsIdentityMaster() bool {
	return n.Identity&IdentityMaster > 0
}

func (n *Pool) IsIdentitySlave() bool {
	return n.Identity&IdentitySlave > 0
}

func (n *Pool) IsIdentityNotDetermined() bool {
	return n.Identity == IdentityNotDetermined
}
