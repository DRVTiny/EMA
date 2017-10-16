package Emas::Handler::FailsReport;
use 5.16.1;
use Memoize;
use S::Expr qw(sexpr);
use JSON::XS qw(decode_json);
use File::Slurp qw(write_file);
use Log4perl::KISS;
use Email::MIME;
use constant {
 ZBX_REPORTS_HERE=>'/var/log/reports/zabbix',
};

memoize('expr');

sub new {
  bless {'expr'=>'(| rcpt_to mail_from)'}, shift
}

sub rcpt_to {
  return \qr/opkc-fails-report\@/
}

sub mail_from {
  return \qr/^ohh-me\@/
}

sub expr {
  my $slf=shift;
  sexpr($slf->{'expr'})
}

sub work {
  my ($slf, $mail, $opt)=@_;
  info_('This mail is from opkc, which is far-far away from here...');
  error_('Cant parse mail with Email::MIME'), return
      unless my $parsedMail = Email::MIME->new($mail->{'data'});
  my $flFoundReportAttach=0;
  $parsedMail->walk_parts(sub {
      my ($msgPart)=@_;
      return if $msgPart->subparts or $msgPart->content_type !~ m%application/json%i;
      do { 
          error_(q(Filename not specified properly within the attachment header! I've got << %s >> instead of some *_events_*), $msgPart->filename);
          return
      } unless (my $fileName=$msgPart->filename)=~m/_events_/;
      error_('Cant decode JSON attachment'), return
           unless my $rep=eval { decode_json($msgPart->body) };
      
      write_file(
        my $outFilePath=scalar($opt->{'reports_here'} // ZBX_REPORTS_HERE).'/'.($fileName=~s%\.dat$%%?$fileName.'.json':$fileName),
        $msgPart->body
      );
      info_('Report was written to: %s', $outFilePath);
      $flFoundReportAttach++;
  });
  error_('Cant find report in attachments to this mail') unless $flFoundReportAttach;
}

sub opts {
  return ( [ 'reports-here|R=s', 'place report files here, please', {'default'=>ZBX_REPORTS_HERE} ] )
}

1;
