#!/usr/bin/perl
use v5.16.1;
use utf8;
use strict;
use warnings;
use lib '/opt/Perl5/libs';

use constant {
    DFLT_LOG_FILE=>'ae-mail-server.log',
    DFLT_SMTP_PORT=>25,
    DFLT_OUR_DOMAIN=>'mail.example.com',
    DFLT_PID_FILE=>q|/tmp/ae-mail-server.pid|,
    DFLT_WORK_DIR=>q|/tmp|,
};
use POSIX;
use Getopt::Long::Descriptive;
use Carp;
use IO::Handle;
use AnyEvent::Strict;
use AnyEvent::SMTP::Server;
use Log4perl::KISS;
use JSON::XS qw(encode_json decode_json);
use Time::HiRes qw(time sleep);
use Date::Parse qw(str2time);
use Data::Dumper;
use Try::Tiny;
use Email::Simple;
use YAML::XS;
use PPI;
use File::Slurp;
use File::SafeOps::PID qw(createPIDFile);
use Getopt::Std qw(getopts);
sub calc_expr;
sub doLoadHandlers ($$$);

binmode $_, ':utf8' for *STDOUT, *STDERR;

my $dfltConfigPath=$ENV{'EMAS_CONFIG'} // scalar(($0=~m%([^/.]+)(?:\.[^/]*)?$%)[0]).'.yml';
my $confPath=do {
  local @ARGV=@ARGV;
  open( local *STDERR, '>', \(my $stderr) );
  getopts ':c:', \my %o;
  $o{'c'}
};
my $conf=Load do { 
  $confPath//=$dfltConfigPath;
  open my $fhConf, '<', $confPath;
  local $/=<$fhConf>
};

my %smtpDataPart=(
  'rcpt_to'=>{
    'pass'=>sub {
      join("\n"=>@{shift->{'to'}})
    },
    'index'=>10,
  },
  'mail_from'=>{
    'pass'=>sub {
      shift->{'from'}
    },
    'index'=>20,
  },
  'body_to'=>{
    'pass'=>sub {
      my $mp=Email::Simple->new(shift->{'data'});
      $mp->header('To')
    },
    'index'=>30,
  },
  'body_from'=>{
    'pass'=>sub {
      my $mp=Email::Simple->new(shift->{'data'});
      $mp->header('From')
    },
    'index'=>40,
  },
  'body'=>{
    'pass'=>sub {
      shift->{'data'}
    },
    'index'=>50,
  },
);

my %func_args_n=(
  'and'=>{
    'argsn_min'=>2,
    'sname'=>'&',
    'short'=>0
  },
  'or'=>{
    'argsn_min'=>2,
    'sname'=>'|',
    'short'=>1,
  },
  'not'=>{
    'argsn_eq'=>1,
    'sname'=>'!',
    'eval'=>sub { 1-$_[1] },
  },
  'xor'=>{
    'argsn_min'=>2,
    'sname'=>'^',
    'eval'=>sub { $_[0]^$_[1] },
  },
  'is'=>{
    'argsn_eq'=>1,
    'sname'=>'~',
    'eval'=>sub { $_[1] },
  }
);
$func_args_n{$_->{'sname'}}=$_ for values %func_args_n;


my @mailPartsCheckOrder=map $_->[0], sort { $a->[1]{'index'} <=> $b->[1]{'index'} } map [each %smtpDataPart], 1..keys %smtpDataPart;
my @opts=(
      [ 'c=s',	'Path to config file', {'default'=>$dfltConfigPath}],
      [ 'listen|l=s', 'hostname or ip address to listen incoming connections on', { 'default' => $conf->{'connection'}{'listen'} // '0.0.0.0' } ],
      [ 'port|p=i',   'port number to bind to',   { 'default'  => $conf->{'connection'}{'port'} // DFLT_SMTP_PORT } ],
      [ 'logfile|L=s', 'log file to use', { 'default' => $conf->{'logging'}{'file'} // DFLT_LOG_FILE }],
      [ 'domain|D=s', 'mail domain name to service', { 'default' => $conf->{'smtp'}{'mydomain'} // $conf->{'smtp'}{'domain'} // DFLT_OUR_DOMAIN } ],
      [ 'background|bg|b', 'Run in background' ],
      [ 'pid-file|P=s', 'Path to the PID file', {'default' => $conf->{'files'}{'pid'} // DFLT_PID_FILE }],
      [ 'workdir|w=s', 'Work directory (for deamonized mode of execution)', {'default'=>$conf->{'files'}{'workdir'} // DFLT_WORK_DIR} ],
);

my %hndls;

$_=$conf->{'handlers'}{'load'};
# We have to load handlers here, because some of this handlers may push additional command-line options
doLoadHandlers($conf->{'handlers'}{'path'}, $_, \@opts) if $_ && ref($_) eq 'ARRAY' && @{$_};

my ($opt, $usage) = describe_options(
  $0.' %o [-l HOST_OR_IP] [-p PORT] [-x] [-v] [-h]' =>
      @opts,
      [],
      [ 'verbose|v',  'print extra stuff'            ],
      [ 'help|h',     'print usage message and exit', { 'shortcircuit' => 1 } ],
);
print($usage->text), exit if $opt->help;

if (defined $opt->logfile and $opt->logfile ne '-') {  
  log_open($opt->logfile);
}

if ($opt->background and $opt->pid_file and ref(my $dae=doDaemonize($opt->workdir))) {
  chomp(my $grandChildPid=readline($dae->{'fhPipe'}));
  close $dae->{'fhPipe'};
  info_ 'Daemon forked with the PID %d', $grandChildPid;
  waitpid($dae->{'childPid'}, 0);
  exit;
} else {
  info_ 'Running in foreground'
}

my $ourDomain=$opt->domain;

my $fhPID=$opt->pid_file
  ? try {
      createPIDFile($opt->pid_file)
    } catch {
      logdie_('PID file locked: '.$_)
    }
  : undef;
my $server = AnyEvent::SMTP::Server->new(
    'port' => $opt->port,
    'mail_validate' => sub {
        my ($m,$addr) = @_;
        return 0, 513, 'Bad sender.' unless lc($addr) =~ /[.@]nspk\.ru$/;
        info_("Sender: $addr");
        return 1;
    },
    'rcpt_validate' => sub {
        my ($m,$addr) = @_;
        $addr=lc($addr);
        return 0, 513, 'Bad recipient.'
            unless substr($addr,length($addr)-length($ourDomain)) eq $ourDomain;
        info_("Recipient: $addr");
        return 1;
    },
) || logdie_ 'Cant create instance of AnyEvent::SMTP::Server';

$server->reg_cb(
    'ready' => sub {
        info_('SMTP server is ready to accept connections');
    },
    'client' => sub {
        my ($s,$con) = @_;
        info_("Client from $con->{host}:$con->{port} connected");
    },
    'disconnect' => sub {
        my ($s,$con) = @_;
        info_("Client from $con->{host}:$con->{port} gone");
    },
    'mail' => sub {
        my ($s,$mail) = @_;
        try {
            my $mailTo=join("\n"=>@{$mail->{'to'}});
            info_('Received mail from %s to %s', $mail->{'from'}, $mailTo);
            for my $hndl (grep exists($_->{'checks'}), values %hndls) {
              next unless calc_expr($hndl->{'slf'}->expr, $mail, $hndl->{'checks'});
              $hndl->{'slf'}->work($mail, $opt);
            }
        } catch {
            error_('Catched error: %s',$_);
        } finally {
            debug_('Mail processing finished');
        };
    },
);

$server->start;
AnyEvent->condvar->recv;

sub calc_expr {
  my ($expr, $mail,$checks,$mem)=@_;
  $mem//={};
  return unless defined(my $firstExpr=lc(${$expr->[0]}));
  my ($funcName, $funcDef, $argnShift)=
    exists $func_args_n{$firstExpr} 
      ? ( $firstExpr, 	$func_args_n{ $firstExpr }, 	1 )
      : ( 'is',		$func_args_n{ 'is' },		0 );
  my $argsN=$#{$expr}+1-$argnShift;
  error_('Wrong number of arguments for <<%s>> check. Expr=%s', $funcDef->{'sname'}, Dumper($expr))
    if 	(defined($funcDef->{'argsn_eq'}) and $argsN!=$funcDef->{'argsn_eq'}) 	or
        (defined($funcDef->{'argsn_min'}) and $argsN<$funcDef->{'argsn_min'})	or
        (defined($funcDef->{'argsn_max'}) and $argsN>$funcDef->{'argsn_max'});
  my $gr=defined(my $sh=$funcDef->{'short'})?1-$funcDef->{'short'}:0;
  my $ev=$funcDef->{'eval'};
  for my $op ( @{$expr}[$argnShift..$#{$expr}] ) {
    my $r=
    ref($op) eq 'ARRAY'
      ? calc_expr($op,$mail,$checks,$mem)
      : do {
          my $chk=${$op};
          confess 'Unknown operand: '.$chk unless my $sdp=$smtpDataPart{$chk};
          $mem->{$chk}||=do {
            my $what2chk=$sdp->{'pass'}->($mail);
            given (ref $checks->{$chk}) {
              when('REF') { # $checks->{'mail_from'} (for example) is regexp
                $what2chk=~${$checks->{$chk}}?1:0
              };
              $checks->{$chk}->($what2chk)  	when 'CODE'	;
              default {
                confess 'Unknown check type: '.ref($checks->{$chk})
              }
            }
          }
        };
    # short evaluation for "and" / "or"
    return($sh)
      if defined $sh and $r==$sh;
    # direct evaluation - for "xor" / "not"
    $gr=$ev->($gr,$r) if defined $ev;
  }
  return $gr
}

sub doFork {
    my $pid = fork;
    defined $pid or die "Can't fork: $!\n";
    $pid
}

sub doDaemonize {
    my $workDir=shift // '/';
    pipe(my $fhParentReads, my $fhChildWrites);
    my $childPid=doFork;
    $childPid and
      return {
        'fhPipe'=>$fhParentReads,
        'childPid'=>$childPid
      };
    close $_ for $fhParentReads, *STDIN, *STDOUT;
    POSIX::setsid() or logdie_ "Can't start a new session: $!";
    umask 0;
    chdir($workDir) or logdie_ "Can't chdir to $workDir: $!";
    close STDERR;
    open STDIN , '<', '/dev/null';
    open STDOUT, '>', '/dev/null';
    if ($opt->logfile) {
      open STDERR, '>>', $opt->logfile
    } else {
      open STDERR, '>', '/dev/null'
    }
    $childPid=doFork and do {
      syswrite($fhChildWrites, $childPid);
      close($fhChildWrites);
      exit
    };
    return 1
}

sub doLoadHandlers ($$$) {
  my ($pthHandlers, $loadHandlers, $cmdlOpts)=@_;
  ($pthHandlers//='.')=~s%/+$%%;
  for  ( map [ucfirst($_), join(''=>$pthHandlers,'/',$_,'.pm')], @{$loadHandlers} ) {
    my ($handler, $pth2pm)=@{$_};
    require $pth2pm or die "Cant load handler $handler";
    die "Handler from file << $pth2pm >> seems to be invalid: no 'package' operator found" 
      unless my $loadedPackage=do {
        my $d=PPI::Document->new(\read_file($pth2pm)) || die $!;
        $d->find_first('PPI::Statement::Package')->namespace
      };
    my $oHndl=$hndls{$loadedPackage}{'slf'}=eval "${loadedPackage}->new"
      or logdie_ 'Cant initialize handler from package %s: %s', $loadedPackage, $@;
    unless ($oHndl->can('work')) {
      error_ "Handler $loadedPackage is useless, because it cant do 'work'";
      next
    }
    $hndls{$loadedPackage}{'checks'}{$_}=$oHndl->can($_)->() for grep { $oHndl->can($_) } keys %smtpDataPart;
    if ($oHndl->can('opts')) {
      push @{$cmdlOpts}, $oHndl->opts;
      debug { 'Options from handler %s attached: %s', $handler, Dumper([$oHndl->opts]) };
    }
    info_ 'Handler %s was loaded', $handler
  }  
}

END {
  eval q|$fhPID|;
  ! $@ and defined($fhPID) and $fhPID->close_me;
}
